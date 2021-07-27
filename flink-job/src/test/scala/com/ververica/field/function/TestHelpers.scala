package com.ververica.field.function

import java.io.File

import scala.reflect.io.Directory

object TestHelpers {
  def assertExpectedEqualsActual(expectedOutputData: String, actualOutputDataDir: String, wrappedActual: Boolean = true): Unit = {
    val expected: Array[String] = TestHelpers.loadWholeText(expectedOutputData).split("\\n").sorted
    val actual: Array[String] = TestHelpers.loadAndConcatFilesInDir(actualOutputDataDir)
      .flatMap(line => line.split("\\n"))
      .sorted
    val actualStripEvent: Array[String] =
      if (wrappedActual)
        actual.map(line => line.trim.dropWhile(c => c != '(').drop(1).dropRight(1))
      else
        actual

    assert(expected.length == actualStripEvent.length,
      f"Expected count: ${expected.length} not equal to actual count: ${actualStripEvent.length}")
    expected.zip(actualStripEvent).foreach(lineTuple =>
      assert(lineTuple._1.trim == lineTuple._2.trim,
        f"""Expected "${lineTuple._1.trim}" not equal to actual "${lineTuple._2.trim}"""")
    )
  }

  def loadAndConcatFilesInDir(dir: String): Array[String] = {
    val files = new Directory(new File(dir)).files
    val filesContents: Array[String] = files.map(file => loadWholeText(file.path)).toArray
    filesContents.filter(fileContents => !fileContents.isEmpty)
  }

  def loadWholeText(path: String): String = {
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }

  def deleteDir(actualOutputData: String) = {
    new Directory(new File(actualOutputData)).deleteRecursively()
  }
}
