package com.fairlink

import com.fairlink.constants.Constants
import com.fairlink.core.implementation.FairDistributorJob

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}


object FairDistributor {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw FairDistributorException("You must provide argument odate in the format yyyy-MM-dd")
    }
    val odate = args(0)
    validateOdate(odate)

    val job = FairDistributorJob(odate)
    job.run()
  }

  private def validateOdate(odate: String) = {
    try {
      LocalDate.parse(odate, DateTimeFormatter.ofPattern(Constants.DATE_FORMAT))
    } catch {
      case dtpe: DateTimeParseException => throw FairDistributorException("You must provide argument odate in the format yyyy-MM-dd")
    }
  }
}
