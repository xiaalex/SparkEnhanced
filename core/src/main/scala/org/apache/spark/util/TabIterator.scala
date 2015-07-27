/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import scala.util.matching.Regex
import scala.collection.Iterator.empty

abstract class AbstractIterator[+A] extends Iterator[A]

class TabIterator[T](index: Int, iter: Iterator[T]) extends Iterator[T] {
  self =>
  private val r = """(\w+)\b""".r

  def hasNext : Boolean = iter.hasNext
  def next : T = iter.next()

  override def map[B](f: T => B): Iterator[B] = new AbstractIterator[B] {
    def hasNext = self.hasNext
    def next() = {
      val st = self.next.toString
      val arr = (r findAllIn st).toArray
      val b = arr(index)
      val c = (f(b.asInstanceOf[T])).toString
      arr(index) = c
      val ret = arr.mkString("\t")
      ret.asInstanceOf[B]
    }
  }

  override def filter(f: T => Boolean): Iterator[T] = new AbstractIterator[T] {
    private var hd: T = _
    private var hdDefined: Boolean = false

    private def col(h: T): T = {
      val st = h.toString
      val arr = (r findAllIn st).toArray
      val b = arr(index)
      b.asInstanceOf[T]
    }

    def hasNext: Boolean = hdDefined || {
      do {
        if (!self.hasNext) return false
        hd = self.next()
      } while (!f(col(hd)))
      hdDefined = true
      true
    }

    def next() = {
      if (hasNext) { 
        hdDefined = false; 
        hd 
      } else {
        empty.next()
      }
    }
  }
}

