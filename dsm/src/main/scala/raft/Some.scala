package raft

import cats._
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
    def id: String
    def requestVote(candidate: Candidate[F] with Node[F]): F[Boolean]
    def appendEntries: F[Unit]
  }

  trait Leader[F[_]] {
    sealed trait LeaderCommmand

    def run: F[Follower[F]]
  }

  trait Follower[F[_]] {
    def run: F[Candidate[F]]
  }

  trait Candidate[F[_]] { this: Node[F] =>
    def run: F[Either[Follower[F], Leader[F]]]
  }

  /////////////////////////////////

  object Leader {
    def make[F[_]: Sync: Timer: Concurrent](node: Node[F])(implicit cluster: Cluster[F]): Leader[F] =
      new Leader[F] {
        override def run: F[Follower[F]] = {
          val loop = for {
            _       <- Sync[F].delay(println(s"Node ${node.id} is a Leader"))
            nodes   <- cluster.nodes
            workers <- nodes.map(node => Concurrent[F].start(node.appendEntries)).sequence.map(_.map(_.join))
            _       <- Timer[F].sleep(1.second)
          } yield ()

          loop.foreverM
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
    )(implicit cluster: Cluster[F]): F[Node[F] with Candidate[F]] = {
      for {
        anotherLeaderElected <- Deferred[F, Unit]
      } yield {
        new Node[F] with Candidate[F] {
          type Timeout = Unit
          val timeoutDuration = 3000.millis

          override def id: String                                                    = node.id
          override def requestVote(candidate: Candidate[F] with Node[F]): F[Boolean] = node.requestVote(candidate)
          override def appendEntries: F[Unit] =
            Sync[F].delay(println(s"${node.id}: received append entries")) *> node.appendEntries *> anotherLeaderElected
              .complete(())

          override def run: F[Either[Follower[F], Leader[F]]] = {
            election.flatMap {
              case Timeout              => run
              case NotChosen            => run
              case AnotherLeaderElected => Follower.make[F](node).asLeft[Leader[F]].pure[F]
              case Leadership           => Leader.make[F](node).asRight[Follower[F]].pure[F]
            }
          }

          def election: F[ElectionResult] =
            for {
              nodes  <- cluster.nodes
              result <- collectVotes(nodes, timeoutDuration, anotherLeaderElected)
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
              .flatMap(_.join.flatTap(v => Sync[F].delay(println(s"${node.id}: $v"))).map {
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
    def make[F[_]: Sync: Timer](node: Node[F]): Follower[F] =
      new Follower[F] {
        override def run: F[Candidate[F]] =
          (Sync[F].delay(println(s"Node ${node.id} is a Follower")) *> Timer[F].sleep(1.second)).foreverM
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

  class Lifetime[F[_]: Concurrent: Timer](implicit cluster: Cluster[F]) {
    def node(node: Node[F]): F[Unit] = Candidate.apply[F](node).flatMap(candidate(_))

    def candidate(candidate: Candidate[F]): F[Unit] = {
      candidate.run.flatMap {
        case Left(f)  => follower(f)
        case Right(l) => leader(l)
      }
    }

    def follower(follower: Follower[F]): F[Unit] =
      follower.run.flatMap(candidate(_))

    def leader(leader: Leader[F]): F[Unit] =
      leader.run.flatMap(follower(_))
  }

  object Node {
    def make[F[_]: Applicative: Concurrent: Timer](nodeId: String): Node[F] =
      new Node[F] {
        override def id: String = nodeId
        override def requestVote(candidate: Candidate[F] with Node[F]): F[Boolean] =
          Sync[F].delay(println(s"$id: Node ${candidate.id} requested vote")) *> Timer[F].sleep(
            1500.millis
          ) *> (candidate.id === "A").pure[F]
        override def appendEntries: F[Unit] = Sync[F].delay(println("Base append entries")) *> Applicative[F].unit
      }
  }
}
