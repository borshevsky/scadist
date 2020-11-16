package my.util

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.concurrent._

package object concurrent {
  def racePredN[F[_]: Concurrent, A](
      es: List[F[A]],
      N: Int
  )(pred: A => Boolean): F[List[A]] =
    for {
      f <- Ref.of[F, Int](0)
      r <- Deferred[F, Unit]

      values <- raceN(es, f, r, N)(pred)
      _ <- r.get
      got <- f.get
    } yield (values)

  def race3[F[_]: Concurrent, A, B, C](a: F[A], b: F[B], c: F[C]): F[(Option[A], Option[B], Option[C])] =
    Concurrent[F].race(a, Concurrent[F].race(b, c)) flatMap {
      case Left(a)         => (a.some, none[B], none[C]).pure[F]
      case Right(Left(b))  => (none[A], b.some, none[C]).pure[F]
      case Right(Right(c)) => (none[A], none[B], c.some).pure[F]
    }

  private type D[F[_]] = Deferred[F, Unit]
  private type R[F[_]] = Ref[F, Int]

  private def waitUntilN[F[_]: Monad, A](
      e: F[A],
      f: R[F],
      r: D[F],
      N: Int
  )(pred: A => Boolean): F[Option[A]] =
    e.flatMap { a =>
      if (pred(a)) f.getAndUpdate(_ + 1).flatMap { prev =>
        completeIf[F](f, r)(prev + 1 == N)
      } *> a.some.pure[F]
      else none.pure[F]
    }

  private def completeIf[F[_]: Applicative](
      f: R[F],
      r: D[F]
  )(pred: => Boolean): F[Unit] =
    if (pred) r.complete(())
    else ().pure[F]

  private def raceN[F[_], A](
      es: List[F[A]],
      f: R[F],
      r: D[F],
      N: Int
  )(pred: A => Boolean)(implicit C: Concurrent[F]): F[List[A]] =
    es.traverse { e =>
      C.race(waitUntilN(e, f, r, N)(pred), r.get)
    }.map(_.collect {
      case Left(Some(v)) => v
    })
}
