import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._

import raft._

object Main extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    type F[T] = Task[T]

    val A = Node.make[F]("A")
    val B = Node.make[F]("B")
    val C = Node.make[F]("C")

    implicit val cluster = Cluster.make[F](List(A, B, C))

    val CA = Candidate.make[F](A)

    CA.election.exitCode
  }
}
