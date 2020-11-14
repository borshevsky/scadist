package raft

import cats._
import cats.implicits._
import cats.effect._
import scala.concurrent.duration._

import my.util.concurrent._

case class Term(epoch: Int)
case class Bullet(voteGranted: Boolean, term: Term)

trait Node[F[_]] {
  def requestVote(from: Node[F]): F[Bullet]
}

object Node {
  def make[F[_]: Applicative: Timer](id: String): Node[F] =
    new Node[F] {
      override def requestVote(from: Node[F]): F[Bullet] =
        Timer[F].sleep(1.second) *>
          Applicative[F].pure(Bullet(true, Term(0)))
    }
}

trait Leader[F[_]] {
  def heartbeat(cluster: Cluster[F]): F[Unit]
}

trait Candidate[F[_]] {
  def election: F[Unit]
}

object Candidate {
  def make[F[_]: Monad: Sync: Concurrent](
      self: Node[F]
  )(implicit cluster: Cluster[F]): Candidate[F] =
    new Candidate[F] {
      override def election: F[Unit] = {
        for {
          _ <- Sync[F].delay(println("Start election..."))
          _ <- requestVotes.flatMap { vs =>
            racePredN(vs, vs.length / 2 + 1)(_.voteGranted)
          }
          _ <- Sync[F].delay(println("Majority received"))
        } yield ()
      }
      def requestVotes: F[List[F[Bullet]]] =
        cluster.nodes.map(_.map(_.requestVote(self)))
    }
}
