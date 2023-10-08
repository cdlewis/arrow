package arrow

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.collection.mutable


sealed trait Arrow[In, Out] {
    def Run(input: In): Out

    def Map[NewOut](fn: Out => NewOut): Arrow[In, NewOut] = new MapArrow(this, fn)

    def AndThen[NewOut](child: Arrow[Out, NewOut]): Arrow[In, NewOut] = new AndThenArrow(this, child)

    def Join(another: Arrow[In, Out]): Arrow[In, Seq[Out]] = new JoinArrow(this, another)

    def Parent(): Seq[Arrow[_, _]]

}

sealed trait BatchableArrow[In, Out] extends Arrow[In, Out] {

}

case object Arrow {
    def Identity[In](): IdentityArrow[In] = new IdentityArrow[In]()

    def PrintGraph[In, Out](root: Arrow[In, Out]): Unit = {
        val (_, adjMap) = Graph.buildAdjacencyMap(Queue(root), Map[Arrow[_, _], Set[Arrow[_, _]]]())
        Graph.printGraph(adjMap)
//        adjMap.filter(_._2.isEmpty).foreach {
//            case (key, value) =>
//                println(index(key).getClass)
//        }
    }
}

case class Graph()

case object Graph {
    def printGraph(graph: Map[Arrow[_, _], Set[Arrow[_, _]]]): Unit = {
        val seen = new mutable.HashSet[Arrow[_, _]]()
        val q = new mutable.Queue[(Arrow[_, _], Int)]()
        q.enqueueAll(graph.filter(_._2.isEmpty).keys.map((_, 0)))

        while (q.nonEmpty) {
            val (head, indentation) = q.dequeue()

            if (!seen.contains(head)) {
                seen.add(head)
                println("    " * indentation + head.getClass)

                q.enqueueAll(graph.filter(_._2.diff(seen).isEmpty).keys.map((_, indentation+1)))
            }

        }
    }

    def buildAdjacencyMap(
        queue: Queue[Arrow[_, _]],
        currentMap: Map[Arrow[_, _], Set[Arrow[_, _]]],
    ): (Queue[Arrow[_, _]], Map[Arrow[_, _], Set[Arrow[_, _]]]) = {
        if (queue.isEmpty) {
            (queue, currentMap)
        } else {
            val (head, rest) = queue.dequeue

            val newMap = currentMap + (head -> head.Parent().toSet)

            head match {
                case andThenArrow: AndThenArrow[_, _, _] =>
                    val subGraph: Map[Arrow[_, _], Set[Arrow[_, _]]] = buildAdjacencyMap(Queue[Arrow[_, _]](andThenArrow.Child()), Map[Arrow[_, _], Set[Arrow[_, _]]]())
                        ._2
                        .map {
                            case (key, value) if value.isEmpty => (key, Set(head))
                            case x => x
                        }

                    buildAdjacencyMap(
                        rest.enqueueAll(head.Parent().filterNot(x => currentMap.contains(x))),
                        newMap ++ subGraph
                    )
                case _ =>
                    buildAdjacencyMap(rest.enqueueAll(head.Parent().filterNot(x => currentMap.contains(x))), newMap)
            }
        }
    }
}

class IdentityArrow[In]() extends Arrow[In, In] {
    def Run(input: In): In = input

    def Parent(): Seq[Arrow[_, In]] = Seq.empty
}

class MapArrow[In, Out, NewOut](parent: Arrow[In, Out], fn: Out => NewOut) extends Arrow[In, NewOut] {
    def Run(input: In): NewOut = fn(parent.Run(input))

    def Parent(): Seq[Arrow[_, In]] = Seq(parent.asInstanceOf[Arrow[_, In]])
}

class JoinArrow[In, Out](parents: Arrow[In, Out]*) extends Arrow[In, Seq[Out]] {
    def Run(input: In): Seq[Out] = parents.map(_.Run(input))

    def Parent(): Seq[Arrow[_, _]] = parents.map(_.asInstanceOf[Arrow[_, _]])
}

class AndThenArrow[In, Out, NewOut](parent: Arrow[In, Out], child: Arrow[Out, NewOut]) extends Arrow[In, NewOut] {
    def Run(input: In): NewOut = child.Run(parent.Run(input))

    def Child(): Arrow[Out, NewOut] = child

    def Parent(): Seq[Arrow[_, _]] = Seq(parent.asInstanceOf[Arrow[_, _]])
}

case class SampleRequest(name: String, friends: Seq[String])

object Main {
    def main(args: Array[String]): Unit = {
        val mainUser = Arrow
            .Identity[SampleRequest]()
            .Map(_.name)
            .AndThen(UserService.GetUser())

        val siblings = Arrow
            .Identity[SampleRequest]()
            .Map(_.friends.head)
            .AndThen(UserService.GetUser())

        val handler = mainUser
            .Join(siblings)
            .Map(users => users.map(_.name))
            .Map(_.mkString(" <3 "))

        Arrow.PrintGraph[SampleRequest, String](handler)
        println(handler.Run(SampleRequest(name = "Chris", friends = Seq("Mike", "Lucy"))))
    }
}

case class User(name: String)

class GetUserArrow() extends BatchableArrow[String, User] {
    def Run(input: String): User = User(input)

    def Parent(): Seq[Arrow[_, String]] = Seq.empty
}

object UserService {
    def GetUser(): Arrow[String, User] = new GetUserArrow()
}
