package com.ververica.field.function.sources

case class TestSourceConfig(inputPath: String,
                            processingTimeDelaySeconds: Int,
                            timeSpeedMultiplier: Int,
                            testInputDelimiter: String = ",")
