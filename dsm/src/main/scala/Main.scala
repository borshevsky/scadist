import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._

import cats.effect.concurrent.Deferred

import raft._

object Main extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    type F[+T] = Task[T]

    val A = Node.make[F]("A")
    val B = Node.make[F]("B")
    val C = Node.make[F]("C")

    implicit val cluster = Cluster.make[F](List(A, B, C))

    val CA = Candidate.make[F](A)

    val p = for {
      d <- Deferred[F, Unit]
      v <- CA.election(d).exitCode
    } yield (v)

    p.catchAll(_ => ZIO.succeed(ExitCode.failure))
  }
}
