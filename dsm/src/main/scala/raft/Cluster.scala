package raft

import cats._
import cats.implicits._

trait Cluster[F[_]] {
  def nodes: F[List[Node[F]]]
}

object Cluster {
  def make[F[_]: Applicative](participants: List[Node[F]]) =
    new Cluster[F] {
      override def nodes: F[List[Node[F]]] = participants.pure[F]
    }
}


