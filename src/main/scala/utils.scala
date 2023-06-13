object utils {
  def getSeason(month: Int): String = {
    month match {
      case 12 | 1 | 2 => "Winter"
      case 3 | 4 | 5 => "Spring"
      case 6 | 7 | 8 => "Summer"
      case 9 | 10 | 11 => "Fall"
      case _ => "Unknown"
    }
  }
}
