package raft

import cats._
import cats.data.State
import cats.implicits._
import cats.effect._
import cats.effect.concurrent._

import scala.concurrent.duration._

import fs2.Stream
import fs2.concurrent.Queue
import cats.effect.concurrent.Deferred
import cats.instances.unit

import scala.util.chaining._

object Raft {
  case class AppendEntries()

  trait Node[F[_]] {
    def id: F[String]
    def requestVote(candidate: Candidate[F]): F[Boolean]
    def appendEntries: F[Unit]
  }

  sealed abstract class LocalNode[F[_]](node: Node[F]) extends Node[F] {
    override def appendEntries: F[Unit]                           = node.appendEntries
    override def id: F[String]                                    = node.id
    override def requestVote(candidate: Candidate[F]): F[Boolean] = node.requestVote(candidate)
  }

  abstract class Leader[F[_]](node: Node[F]) extends LocalNode[F](node) {
    def run: F[Follower[F]]
  }

  abstract class Follower[F[_]](node: Node[F]) extends LocalNode[F](node) {
    def run: F[Candidate[F]]
  }

  abstract class Candidate[F[_]](node: Node[F]) extends LocalNode[F](node) {
    def run(peers: List[RemoteNode[F]]): F[Either[Follower[F], Leader[F]]]
  }

  /////////////////////////////////

  object Leader {
    def make[F[_]: Sync: Timer: Concurrent](node: Node[F], peers: List[RemoteNode[F]]): Leader[F] =
      new Leader[F](node) {
        override def run: F[Follower[F]] = {
          val loop = for {
            id      <- node.id
            _       <- Sync[F].delay(println(s"Node ${id} is a Leader"))
            workers <- peers.map(node => Concurrent[F].start(node.appendEntries)).sequence.map(_.map(_.join))
            _       <- Timer[F].sleep(1.second)
          } yield ()

          loop.replicateA(3) *> Timer[F].sleep(15.second) *> Follower.apply[F](node)
        }
      }
  }

  sealed trait ElectionResult
  case object Timeout              extends ElectionResult
  case object AnotherLeaderElected extends ElectionResult
  case object Leadership           extends ElectionResult
  case object NotChosen            extends ElectionResult

  object Candidate {
    def apply[F[_]: Concurrent: Timer](
        node: Node[F]
    ): F[Candidate[F]] = {
      for {
        anotherLeaderElected <- Deferred[F, Unit]
      } yield {
        new Candidate[F](node) {
          type Timeout = Unit
          val timeoutDuration = 3000.millis

          override def appendEntries: F[Unit] = node.appendEntries *> anotherLeaderElected.complete(())

          override def run(peers: List[RemoteNode[F]]): F[Either[Follower[F], Leader[F]]] = {
            election(peers).flatMap {
              case Timeout              => run(peers)
              case NotChosen            => run(peers)
              case AnotherLeaderElected => Follower.apply[F](node).flatMap(f => f.asLeft[Leader[F]].pure[F])
              case Leadership           => Leader.make[F](node, peers).asRight[Follower[F]].pure[F]
            }
          }

          def election(peers: List[RemoteNode[F]]): F[ElectionResult] =
            for {
              result <- collectVotes(peers, timeoutDuration, anotherLeaderElected)
            } yield result

          def collectVotes(
              nodes: List[Node[F]],
              timeout: FiniteDuration,
              anotherLeaderElected: Deferred[F, Unit]
          ): F[ElectionResult] =
            for {
              timeout <- timeoutTo(timeout)
              waiter  <- Waiter.make[F](majority(nodes))
              results <-
                nodes
                  .map(node =>
                    Concurrent[F].start(work(node, timeout.join, waiter.get, anotherLeaderElected.get, waiter))
                  )
                  .sequence
                  .map(_.map(_.join))
              res <- results.sequence
              _   <- Sync[F].delay(println(res))
            } yield res.groupBy(identity).maxBy(_._2.length)._1

          def requestVote(node: Node[F]): F[Fiber[F, Boolean]] = Concurrent[F].start(node.requestVote(this))

          def work(
              node: Node[F],
              timeout: F[Unit],
              majorityCollected: F[Unit],
              leaderElected: F[Unit],
              waiter: Waiter[F]
          ): F[ElectionResult] =
            requestVote(node)
              .flatMap(collect(_, waiter))
              .flatMap(interrupted(timeout, _))
              .flatMap(interrupted(leaderElected, _))
              .flatMap(interrupted(majorityCollected, _))
              .flatMap(_.join.map {
                case Left(_)                    => Leadership
                case Right(Left(_))             => AnotherLeaderElected
                case Right(Right(Left(_)))      => Timeout
                case Right(Right(Right(true)))  => Leadership
                case Right(Right(Right(false))) => NotChosen
              })

          def collect(vote: Fiber[F, Boolean], waiter: Waiter[F]): F[Fiber[F, Boolean]] =
            Concurrent[F].start(vote.join.flatTap {
              case true => waiter.ready *> Sync[F].delay(println(s"${node.id}: One more vote for me"))
              case _    => Applicative[F].unit *> Sync[F].delay(println(s"${node.id}: Vote against me"))
            })

          def interrupted[A](interrupt: F[Unit], vote: Fiber[F, A]): F[Fiber[F, Either[Unit, A]]] =
            Concurrent[F].start(Concurrent[F].race(interrupt, vote.join))

          def timeoutTo(d: FiniteDuration): F[Fiber[F, Unit]] =
            Concurrent[F].start(Timer[F].sleep(d))

          def majority[A](s: Seq[A]) = s.length / 2 + 1
        }
      }
    }
  }

  trait Waiter[F[_]] {
    def ready: F[Unit]
    def get: F[Unit]
  }

  object Waiter {
    def make[F[_]: Sync: Concurrent](count: Int): F[Waiter[F]] =
      for {
        box  <- Ref.of[F, Int](0)
        done <- Deferred[F, Unit]
      } yield new Waiter[F] {
        override def ready: F[Unit] =
          box.updateAndGet(_ + 1).flatMap { value =>
            if (value === count) done.complete(())
            else Applicative[F].unit
          }
        override def get: F[Unit] = done.get <* Sync[F].delay(println("majority!"))
      }
  }

  object Follower {
    def apply[F[_]: Sync: Timer: Concurrent](node: Node[F]): F[Follower[F]] =
      for {
        m <- MVar[F].empty[Unit]
      } yield new Follower[F](node) {
        override def run: F[Candidate[F]] =
          for {
            me      <- id
            _       <- Sync[F].delay(println(s"$me is a Follower"))
            timeout <- Concurrent[F].start(Timer[F].sleep(3000.millis))
            res     <- Concurrent[F].race(timeout.join, m.take)
            r <- res match {
              case Left(value)  => Sync[F].delay(println("Convert to candidate now")) *> Candidate.apply[F](node)
              case Right(value) => Sync[F].delay(println("Received heartbeat")) *> run
            }
          } yield r

        override def appendEntries: F[Unit] = m.put(()) *> node.appendEntries
      }
  }

  trait Cluster[F[_]] {
    def nodes: F[List[Node[F]]]
  }

  object Cluster {
    def make[F[_]: Applicative](peers: List[Node[F]]): Cluster[F] =
      new Cluster[F] {
        override def nodes: F[List[Node[F]]] = peers.pure[F]
      }
  }

  class Worker[F[_]: Concurrent: Timer](me: LocalNode[F]) extends Node[F] {
    override def id: F[String]                                    = me.id
    override def requestVote(candidate: Candidate[F]): F[Boolean] = me.requestVote(candidate)
    override def appendEntries: F[Unit]                           = me.appendEntries

    def run(peers: List[RemoteNode[F]]): F[Worker[F]] =
      me match {
        case c: Candidate[F] => candidate(c, peers)
        case f: Follower[F]  => follower(f, peers)
        case l: Leader[F]    => leader(l, peers)
      }

    def candidate(candidate: Candidate[F], peers: List[RemoteNode[F]]): F[Worker[F]] = {
      candidate.run(peers).map {
        case Left(f)  => new Worker(f)
        case Right(l) => new Worker(l)
      }
    }

    def follower(follower: Follower[F], peers: List[RemoteNode[F]]): F[Worker[F]] =
      follower.run.map(candidate => new Worker(candidate))

    def leader(leader: Leader[F], peers: List[RemoteNode[F]]): F[Worker[F]] =
      leader.run.map(follower => new Worker(follower))
  }

  object Worker {
    def make[F[_]: Concurrent: Timer](me: Node[F]): F[Worker[F]] = {
      Follower.apply[F](me).map(follower => new Worker[F](follower))
    }
  }

  trait RemoteNode[F[_]] extends Node[F]

  trait LocalRemoteNode[F[_]] extends RemoteNode[F] {
    def run(peers: List[RemoteNode[F]]): F[Unit]
  }

  object LocalRemoteNode {
    def apply[F[_]: Concurrent: Timer](node: Node[F]): F[LocalRemoteNode[F]] =
      for {
        worker <- Worker.make[F](node)
        ref    <- Ref.of[F, Worker[F]](worker)
        p      <- Deferred[F, List[RemoteNode[F]]]
        _      <- Concurrent[F].start(loop(ref, p))
      } yield new LocalRemoteNode[F] {
        override def run(peers: List[RemoteNode[F]]): F[Unit] = p.complete(peers)

        override def id: F[String]                                    = ref.get.flatMap(_.id)
        override def requestVote(candidate: Candidate[F]): F[Boolean] = ref.get.flatMap(_.requestVote(candidate))
        override def appendEntries: F[Unit]                           = ref.get.flatMap(_.appendEntries)
      }

    private def loop[F[_]: Monad](ref: Ref[F, Worker[F]], peers: Deferred[F, List[RemoteNode[F]]]): F[Unit] = {
      def go(p: List[RemoteNode[F]]) =
        for {
          w  <- ref.get
          nW <- w.run(p)
          _  <- ref.set(nW)
        } yield ()

      peers.get.flatMap(p => go(p).foreverM)
    }
  }

  object Node {
    def make[F[_]: Applicative: Concurrent: Timer](nodeId: String): Node[F] =
      new Node[F] {
        override def id: F[String] = nodeId.pure[F]
        override def requestVote(candidate: Candidate[F]): F[Boolean] =
          for {
            me  <- id
            cid <- candidate.id
            _   <- Sync[F].delay(println(s"$me: Node $cid requested vote"))
            _   <- Timer[F].sleep(1500.millis)
          } yield cid === "A"
        override def appendEntries: F[Unit] = Sync[F].delay(println("Base append entries")) *> Applicative[F].unit
      }
  }
}
