package raft

import cats._
import cats.implicits._

import fs2.Stream

trait Cluster[F[_]] {
  def nodes: F[List[Node[F]]]
  def messages: Stream[F, Unit]
}

object Cluster {
  def make[F[_]: Applicative](participants: List[Node[F]]) =
    new Cluster[F] {
      override def messages: Stream[F, Unit] = ???
      override def nodes: F[List[Node[F]]] = participants.pure[F]
    }
}
