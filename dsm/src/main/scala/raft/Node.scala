package raft

import cats._
import cats.implicits._
import cats.effect._
import scala.concurrent.duration._

import fs2._

import my.util.concurrent._
import cats.effect.concurrent.Deferred

case class Term(epoch: Int)
case class Bullet(voteGranted: Boolean, term: Term)

trait Node[F[_]] {
  def requestVote(from: Node[F]): F[Bullet]
  def appendEntries: F[Unit]
}

object Node {
  def make[F[_]: Applicative: Timer](id: String): Node[F] =
    new Node[F] {
      override def appendEntries: F[Unit] = ().pure[F]

      override def requestVote(from: Node[F]): F[Bullet] =
        Timer[F].sleep(10.millis) *>
          Applicative[F].pure(Bullet(true, Term(0)))
    }
}

sealed trait NodeState

trait Leader[F[_]] extends NodeState {
  def heartbeat(cluster: Cluster[F]): F[Unit]
  def resolve(initial: Deferred[F, Unit]): F[Unit]
}

object Leader {
  def make[F[_]: Monad: Sync](
      self: Node[F],
  )(implicit cluster: Cluster[F]): Leader[F] =
    new Leader[F] {
      override def resolve(initial: Deferred[F, Unit]): F[Unit] = initial.complete(())
      override def heartbeat(cluster: Cluster[F]): F[Unit] =
        cluster.nodes.map(_.map(_.appendEntries))
    }
}

trait Candidate[F[_]] extends NodeState {
  def election(hearbeat: Deferred[F, Unit]): F[NodeState]
}

object Candidate {
  def make[F[+_]](
      self: Node[F]
  )(implicit cluster: Cluster[F], F: Concurrent[F]): Candidate[F] =
    new Candidate[F] {
      override def election(heartbeat: Deferred[F, Unit]): F[NodeState] = {
        val collectVotes =
          (bullets: List[F[Bullet]]) => racePredN(bullets, bullets.length / 2 + 1)(_.voteGranted)

        def handleVoteResult(
            result: (Option[Unit], Option[Unit], Option[List[Bullet]])
        ): F[NodeState] =
          result match {
            case (Some(_), _, _) => Follower.make[F](self).pure[F]
            case (_, Some(_), _) => election(heartbeat)
            case (_, _, Some(_)) => Leader.make[F](self).pure[F].flatTap(l => l.resolve(heartbeat))
            case _               => F.delay(println("WTF FIX ME PLEASE")) *> election(heartbeat)
          }

        for {
          _ <- F.delay(println("Start election..."))
          electionTimedOut <- startElectionTimeout
          votes <- requestVotes
          electionResult <- race3(heartbeat.get, startElectionTimeout, collectVotes(votes))

          r <- handleVoteResult(electionResult)
          _ <- F.delay(println("Majority received"))
        } yield (r)
      }

      def startElectionTimeout: F[Unit] =
        F.sleep(electionTimeout)

      val electionTimeout = 300.millis

      def requestVotes: F[List[F[Bullet]]] =
        cluster.nodes.map(_.map(_.requestVote(self)))
    }
}

trait Follower[F[_]] extends NodeState {
  def handleMessage[F]: F[Unit]
}

object Follower {
  def make[F[_]](self: Node[F]): Follower[F] = instance[F]

  def create[F[_]: Timer](self: Node[F]): F[Follower[F]] = {
    val timeout = Timer[F].sleep(300.millis)

    for {
      t <- timeout
    }
  }

  def instance[F[_]] = new Follower[F] {

  }
}
