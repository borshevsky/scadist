import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._

import cats._
import cats.implicits._
import cats.effect.{Concurrent}

import cats.effect.concurrent.Deferred

import raft._

object Main extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    type F[A] = Task[A]

    import Raft._

    val nodes = ('A' to 'B').toList.map(id => Node.make[Task](id.toString))

    implicit val cluster = Cluster.make(nodes)

    val lifetime = new Lifetime[F]

    val worker = nodes.map(n => Concurrent[F].start(lifetime.node(n))).sequence

    worker
      .map(_.map(_.join).sequence)
      .flatMap(_ => ZIO.succeed(ExitCode.success))
      .catchAll(_ => ZIO.succeed(ExitCode.failure))
  }
}
