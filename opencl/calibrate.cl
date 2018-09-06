#define dEarthMeanRadius     6371.01	// In km
#define dAstronomicalUnit    149597890	// In km

int position(const int x, const int y, const int x_size){
	return y*x_size + x;
}

bool is_no_data_int(const int value, const int no_data_value) {
    return value == no_data_value;
}

bool is_no_data_float(const float value, const float no_data_value) {
    return isnan(value) || value == no_data_value;
}

double2 satelliteViewAngleToLatLon(const double2 satelliteViewAngle, const double sub_lon){
	double2 xy = radians(satelliteViewAngle);

	double2 sinxy = sin(xy);
	double2 cosxy = cos(xy);

	double cos2y = pown(cosxy.y, 2);
	double sin2y = pown(sinxy.y, 2);
	double cosxcosy = cosxy.x * cosxy.y;
	double cos2yconstsin2y = cos2y + 1.006803 * sin2y;

	double sd_part1 = pown(42164 * cosxcosy, 2);
	double sd_part2 = cos2yconstsin2y * 1737121856;
	double sd = sqrt(sd_part1 - sd_part2);
	double sn_part1 = 42164 * cosxcosy - sd;
	double sn_part2 = cos2yconstsin2y;
	double sn = sn_part1 / sn_part2;
	double s1 = 42164 - sn * cosxcosy;
	double s2 = sn * sinxy.x * cosxy.y;
	double s3 = -1.0 * sn * sinxy.y;
	double sxy = sqrt(pown(s1, 2) + pown(s2, 2));

	double2 latLonRad;
	double lonRad_part = s2 / s1;
	latLonRad.y = atan(lonRad_part) + radians(sub_lon);
	double latRad_part = 1.006804 * s3 / sxy;
	latLonRad.x = atan(latRad_part);

	return degrees(latLonRad);
}

double2 solarAzimuthZenith(const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination, const double2 dLatLon){

		double2 azimuthZenith;

		double dLocalMeanSiderealTime = radians(dGreenwichMeanSiderealTime*15 + dLatLon.y);;
		double dHourAngle = dLocalMeanSiderealTime - dRightAscension;
		double dLatitudeInRadians = radians(dLatLon.x);
		double dCos_Latitude = cos( dLatitudeInRadians );
		double dSin_Latitude = sin( dLatitudeInRadians );
		double dCos_HourAngle= cos( dHourAngle );

		azimuthZenith.y = (acos( dCos_Latitude*dCos_HourAngle*cos(dDeclination) + sin( dDeclination )*dSin_Latitude));


		double dY = -sin( dHourAngle );
		double dX = tan( dDeclination )*dCos_Latitude - dSin_Latitude*dCos_HourAngle;
		azimuthZenith.x = atan2( dY, dX );

		if ( azimuthZenith.x < 0.0 )
			azimuthZenith.x = azimuthZenith.x + M_PI*2;
		//udtSunCoordinates->dAzimuth = udtSunCoordinates->dAzimuth/rad;
		// Parallax Correction
		double dParallax=(dEarthMeanRadius/dAstronomicalUnit)*sin(azimuthZenith.y);
		azimuthZenith.y=(azimuthZenith.y + dParallax);///rad;

	return degrees(azimuthZenith);
}

__kernel void rawToRadianceKernel(__global const short *in_data, __global float *out_data, const float offset, const float slope, const float conversionFactor, const short in_no_data_value, const float out_no_data_value) {
	const int xsize = get_global_size(0);
	const int ysize = get_global_size(1);
	const int xgid = get_global_id(0);
	const int ygid = get_global_id(1);
	const int gid = position(xgid,ygid,xsize);

	const short value = in_data[gid];
	float out_value = out_no_data_value;

	if (!is_no_data_int(value, in_no_data_value)) {
		out_value = (offset + value * slope);
	}

    //printf(" xsize= %d, ysize= %d, xgid= %d, ygid = %d, gid= %d, value = %d, out_value = %.6f, offset = %.6f, slope = %.6f \n", xsize, ysize, xgid, ygid, gid, value, out_value, offset, slope);
	out_data[gid] = out_value;
}


__kernel void azimuthZenithKernel(__global float *azimuth_data, __global float *zenith_data, const double scale_x, const double scale_y, const double origin_x, const double origin_y, const double to_view_angle_fac, const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination, const double sub_lon) {
	const int xsize = get_global_size(0);
	const int ysize = get_global_size(1);
	const int xgid = get_global_id(0);
	const int ygid = get_global_id(1);
	const int gid = position(xgid,ygid,xsize);

	const int2 gid2 = (int2)(xgid, ygid);
	const int2 size2 = (int2)(xsize, ysize);
	const double2 origin2 = (double2)(origin_x ,origin_y);
	const double2 scale2 = (double2)(scale_x, scale_y);

   // printf(" xsize= %d, ysize= %d, xgid= %d, ygid = %d, gid= %d, scale_x = %f, scale_y = %f, toViewAngleFac = %f, dGreenwichMeanSiderealTime = %f, dRightAscension = %f, dDeclination = %f \n", xsize, ysize, xgid, ygid, gid, scale_x, scale_y, to_view_angle_fac, dGreenwichMeanSiderealTime, dRightAscension, dDeclination);


	const double2 geosPosition = convert_double2(gid2) * scale2 + origin2;

  //printf(" geosPosition.x= %f, geosPosition.y= %f", geosPosition.x, geosPosition.y);


	const double2 satelliteViewAngle = geosPosition * to_view_angle_fac * (double2)(-1,1);
	const double2 latLonPosition = satelliteViewAngleToLatLon(satelliteViewAngle, sub_lon);
	const double2 azimuthZenith = solarAzimuthZenith(dGreenwichMeanSiderealTime, dRightAscension, dDeclination, latLonPosition);

	azimuth_data[gid] = azimuthZenith.x;
	zenith_data[gid] = azimuthZenith.y;
}


__kernel void rawToReflectanceWithSolarCorrectionKernel(__global const short *in_data, __global float *out_data, const float offset, const float slope, const double dETSRconst, const double dESD, const double scale_x, const double scale_y, const double origin_x, const double origin_y, const double projectionCooridnateToViewAngleFactor, const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination, const short in_no_data_value, const float out_no_data_value, const double sub_lon) {
	const int xsize = get_global_size(0);
	const int ysize = get_global_size(1);
	const int xgid = get_global_id(0);
	const int ygid = get_global_id(1);
	const int gid = position(xgid,ygid,xsize);

	const short value = in_data[gid];
	float out_value = out_no_data_value;
	if (!is_no_data_int(value, in_no_data_value)) {
		float radiance = (offset + value * slope);

		//RasterInfo should provide GEOS coordinates
        int2 gid2 = (int2)(xgid, ygid);
        int2 size2 = (int2)(xsize, ysize);
        double2 origin2 = (double2)(origin_x ,origin_y);
        double2 scale2 = (double2)(scale_x, scale_y);

       	double2 geosPosition = convert_double2(gid2) * scale2 + origin2;


        double2 satelliteViewAngle = geosPosition * projectionCooridnateToViewAngleFactor * (double2)(-1,1);
        double2 latLonPosition = satelliteViewAngleToLatLon(satelliteViewAngle, ((double) sub_lon));
        double2 azimuthZenith = solarAzimuthZenith(dGreenwichMeanSiderealTime, dRightAscension, dDeclination, latLonPosition);

        out_value = radiance * (dESD * dESD) / (dETSRconst * cos(radians(min(azimuthZenith.y, 80.0))));
	}

	out_data[gid] = out_value;
}


__kernel void rawToReflectanceWithoutSolarCorrectionKernel(__global const short *in_data, __global float *out_data, const float offset, const float slope, const double dETSRconst, const double dESD, const short in_no_data_value, const float out_no_data_value) {
	const int xsize = get_global_size(0);
	const int ysize = get_global_size(1);
	const int xgid = get_global_id(0);
	const int ygid = get_global_id(1);
	const int gid = position(xgid,ygid,xsize);

	const short value = in_data[gid];
	float out_value = out_no_data_value;
	if (!is_no_data_int(value, in_no_data_value)) {
		out_value = (offset + value * slope) * (dESD * dESD) / dETSRconst;
	}
    //printf(" xsize= %d, ysize= %d, xgid= %d, ygid = %d, gid= %d, value = %f, out_value = %f, esd = %f, etsr = %f \n", xsize, ysize, xgid, ygid, gid, value, out_value, dESD, dETSRconst);


	out_data[gid] = out_value;
}

__kernel void rawToBbtKernel(__global const short *in_data, __global float *out_data, __global double *rawToBbtLut, const short in_no_data_value, const float out_no_data_value) {
	const int xsize = get_global_size(0);
	const int ysize = get_global_size(1);
	const int xgid = get_global_id(0);
	const int ygid = get_global_id(1);
	const int gid = position(xgid,ygid,xsize);

	const short value = in_data[gid];
	float out_value = out_no_data_value;
	if (!is_no_data_int(value, in_no_data_value)) {
	    out_value = rawToBbtLut[value];
	}

	out_data[gid] = out_value;
}

__kernel void co2CorrectionKernel(__global const float *in_data_bt039, __global const float *in_data_bt108, __global const float *in_data_bt134, __global float *out_data, const float in_no_data_value, const float out_no_data_value) {
	const int xsize = get_global_size(0);
	const int ysize = get_global_size(1);
	const int xgid = get_global_id(0);
	const int ygid = get_global_id(1);
	const int gid = position(xgid,ygid,xsize);

	const float value_bt039 = in_data_bt039[gid];
	const float value_bt108 = in_data_bt108[gid];
	const float value_bt134 = in_data_bt134[gid];

	float out_value = out_no_data_value;
	if (!(is_no_data_float(value_bt039, in_no_data_value) || is_no_data_float(value_bt108, in_no_data_value) || is_no_data_float(value_bt134, in_no_data_value))) {
        const float DTCo2 = (value_bt108 - value_bt134) / 4.0f;
        const float RCorr = pown(value_bt108, 4) - (pown(value_bt108 - DTCo2, 4));
        const float BT039 = powr(pown(value_bt039, 4) + RCorr, 0.25f);
        out_value = BT039;
	}

	out_data[gid] = out_value;
}