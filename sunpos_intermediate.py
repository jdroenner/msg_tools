from numpy.core.umath import sin, cos, pi, arctan2, arcsin, arccos

def sunposIntermediate(iYear,iMonth,iDay,dHours,dMinutes,dSeconds):
    # Calculate difference in days between the current Julian Day
    # and JD 2451545.0, which is noon 1 January 2000 Universal Time

    # Calculate time of the day in UT decimal hours
    dDecimalHours = dHours + (dMinutes + dSeconds / 60.0 ) / 60.0
    # Calculate current Julian Day
    liAux1 =(iMonth-14)/12
    liAux2=(1461*(iYear + 4800 + liAux1))/4 + (367*(iMonth - 2-12*liAux1))/12- (3*((iYear + 4900 + liAux1)/100))/4+iDay-32075
    dJulianDate=liAux2-0.5+dDecimalHours/24.0
    # Calculate difference between current Julian Day and JD 2451545.0
    dElapsedJulianDays = dJulianDate-2451545.0

    # Calculate ecliptic coordinates (ecliptic longitude and obliquity of the
    # ecliptic in radians but without limiting the angle to be less than 2*Pi
    # (i.e., the result may be greater than 2*Pi)
    dOmega=2.1429 - 0.0010394594 * dElapsedJulianDays
    dMeanLongitude = 4.8950630 + 0.017202791698 * dElapsedJulianDays # Radians
    dMeanAnomaly = 6.2400600 + 0.0172019699 * dElapsedJulianDays
    dEclipticLongitude = dMeanLongitude + 0.03341607*sin(dMeanAnomaly) + 0.00034894*sin(2*dMeanAnomaly) - 0.0001134 - 0.0000203*sin(dOmega)
    dEclipticObliquity = 0.4090928 - 6.2140e-9 * dElapsedJulianDays + 0.0000396 * cos(dOmega)

    # Calculate celestial coordinates ( right ascension and declination ) in radians
    # but without limiting the angle to be less than 2*Pi (i.e., the result may be
    # greater than 2*Pi)
    dSin_EclipticLongitude = sin(dEclipticLongitude)
    dY = cos(dEclipticObliquity) * dSin_EclipticLongitude
    dX = cos(dEclipticLongitude)
    dRightAscension = arctan2(dY,dX)
    if dRightAscension < 0.0:
        dRightAscension = dRightAscension + 2*pi
    dDeclination = arcsin(sin(dEclipticObliquity) * dSin_EclipticLongitude)

    dGreenwichMeanSiderealTime = 6.6974243242 + 0.0657098283 * dElapsedJulianDays + dDecimalHours
    return (dRightAscension,dDeclination,dGreenwichMeanSiderealTime)


def sun_earth_distance(date_time):
    day_of_year = date_time.timetuple().tm_yday
    return 1.0 - 0.0167 * cos(2.0 * arccos(-1.0) * ((day_of_year - 3.0) / 365.0))
