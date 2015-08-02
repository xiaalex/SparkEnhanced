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

import java.io._

class DataBlock(delimiter: String, schema: Seq[String], data: String)
  extends Serializable {

  private val dbInfo: DataBlockInfo = new DataBlockInfo(delimiter, schema)
  protected var datalist: Array[String] = dbInfo.createColArray(data)
  protected var index: Int = 0
  private var datatype: String = "String"

  def apply(i: Int): DataBlock = {
    index = i
    this
  }

  def apply(s: String): DataBlock = {
    index = dbInfo.getColIndex(s)
    this
  }

  /** Add string to datablock. */
  def +(x: String): DataBlock = {
    datatype = dbInfo.getColType(index)

    datatype match {
      case "String" => {
          datalist(index) = datalist(index) + x
        }
      case "Int"    => {
        val v: Option[(Int, Int)] = try {
            Some((datalist(index).toInt, x.toInt))
          } catch {
            case e: NumberFormatException => None
          }
        if (v != None) {
          datalist(index) = (v.get._1 + v.get._2).toString
        }
      }
    }
    this
  }

  /** Add int to datablock. */
  def +(x: Int): DataBlock = {
    this.+(x.toString)
  }

  /** Compare string with column in datablock. */
  def >(x: String): Boolean = {
    datatype = dbInfo.getColType(index)

    val v = datatype match {
      case "String" => {
          datalist(index) > x
        }
      case "Int"    => {
        val v: Option[(Int, Int)] = try {
            Some((datalist(index).toInt, x.toInt))
          } catch {
            case e: NumberFormatException => None
          }
        if (v != None) {
          v.get._1 > v.get._2
        }
        else 
          true
      }
    }
    v
  }

  /** Compare int with column in datablock. */
  def >(x: Int): Boolean = {
    this.>(x.toString)
  }

  override def toString = {
    if (datalist != null) {
      index + " : " + datalist.mkString(" ")
    }
    else if (index != 0) { 
      index.toString
    }
    else
      ""
  }

  class DataBlockInfo(delimiter: String, schema: Seq[String]) extends Serializable {

    // For delimited file
    private var sourceDelimiter: String = delimiter
    private var sourceName: Seq[String] = schema.map( x => x.split(":")(0) )
    private var sourceType: Seq[String] = schema.map( x => x.split(":")(1) )

    def getColType(ind: Int): String = {
      sourceType(ind)
    }

    def getColType(name: String): String = {
      sourceType(sourceName.indexOf(name))
    }

    def getColIndex(name: String): Int = {
      sourceName.indexOf(name)
    }
    def createColArray(s: String): Array[String] = {
      s.split(delimiter)
    }
  }

}


