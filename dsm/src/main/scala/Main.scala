import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._

import cats.effect.concurrent.Deferred

import raft._

object Main extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] = ???
}
