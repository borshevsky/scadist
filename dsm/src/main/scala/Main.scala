import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._

import cats._
import cats.implicits._
import cats.effect.{Concurrent, Sync}

import cats.effect.concurrent.Deferred

import raft._

object Main extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    type F[A] = Task[A]

    import Raft._

   val nodes = ('A' to 'C').toList.map(id => Node.make[Task](id.toString))

   val program = for {
     remotes <- nodes.map(node => LocalRemoteNode.apply[F](node)).sequence
     f <- remotes.parTraverse(r => r.run(remotes.filter(n => n != r)))
   } yield f

   program.flatMap(fs =>
      fs.map(_.join).sequence
        .onInterrupt(fs.map(_.cancel).sequence.catchAll(_ =>  ZIO.effectTotal(println("Cancelled")) *> ZIO.succeed(ExitCode.failure)))
        .as(ExitCode.success)
   ).catchAll(_ => ZIO.succeed(ExitCode.failure))
  }
}
