/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.engine.spark2;

import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.common.SparkQueryTest;
import org.junit.Assert;
import org.junit.Test;

public class BuildAndQueryEmptySegmentsTest extends LocalWithSparkSessionTest {

    private static final String CUBE_NAME = "ci_left_join_cube";
    private static final String SQL = "SELECT COUNT(1) as TRANS_CNT from TEST_KYLIN_FACT";

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        System.setProperty("spark.local", "true");
        //System.setProperty("calcite.debug", "true");
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    public void testEmptySegments() throws Exception {
        String project = "dc_project";
        String sql1 = "select * from fact_gps_mileage where vehicle_id=1125239279394902016";
        String sql1_1 = "select dim_date.year_key, dim_date.date_key, fact_gps_mileage.org_id, fact_gps_mileage.vehicle_id, fact_gps_mileage.mileage from fact_gps_mileage as fact_gps_mileage left join dim_date as dim_date on fact_gps_mileage.send_date_id=dim_date.date_key -- group by year_key, date_key, org_id, vehicle_id";
        String sql1_2 = "select send_date_id, count(1) from fact_gps_mileage group by send_date_id order by send_date_id";
        String sql2 = "select send_date_id, year_key, org_name, sum(mileage) as mileages from fact_gps_mileage a left join dim_org b on a.org_id = b.org_id left join dim_date c on a.send_date_id=c.date_key group by send_date_id,year_key,org_name having sum(mileage)>0 order by 3";
        String sql2_1 = "select send_date_id, year_key, org_name, highway_type_name, sum(mileage) as mileages from fact_gps_mileage a left join dim_org b on a.org_id = b.org_id left join dim_date c on a.send_date_id=c.date_key left join dim_highway_type d on a.highway_type_id=d.highway_type_id group by send_date_id,year_key,org_name,highway_type_name having sum(mileage)>0 order by 3";
        String sql3 = "select sum(mileage) as mileages from fact_gps_mileage";
        String sql4 = "select city_name, sum(mileage) as mileages, count(distinct vehicle_id) as cd1 from fact_gps_mileage a left join dim_district b on a.district_id=b.district_id group by city_name order by 1";
        String sql4_1 = "select city_name, sum(mileage) as mileages, count(distinct device_id) as cd1 from fact_gps_mileage a left join dim_district b on a.district_id=b.district_id group by city_name order by cd1 desc";
        String sql5 = "select send_date_id, city_name, vehicle_id, sum(mileage) as mileages from fact_gps_mileage a left join dim_district b on a.district_id=b.district_id where city_name in ('上海市') group by send_date_id, city_name, vehicle_id order by mileages desc limit 1000";
        String sql6 = "select send_date_id, vehicle_id, sum(mileage) as mileages from fact_gps_mileage group by send_date_id, vehicle_id order by mileages desc limit 100";
        String sql7 = "select device_id, percentile(driving_time, 0.5) as percentile from fact_gps_mileage group by device_id order by device_id";
        String dimSql = "select * from dim_highway_type";
        Dataset ds = NExecAndComp.sql(project, sql5);
        ds.show(200);

//        cleanupSegments(CUBE_NAME);
//        buildCuboid(CUBE_NAME, new SegmentRange.TSRange(dateToLong("2009-01-01"), dateToLong("2009-06-01")));
//        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME).getSegments().get(0).getInputRecords());
//
//        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());
//
//        testQueryUnequal(SQL);
//
//        buildCuboid(CUBE_NAME, new SegmentRange.TSRange(dateToLong("2012-06-01"), dateToLong("2015-01-01")));
//        Assert.assertNotEquals(0, cubeMgr.getCube(CUBE_NAME).getSegments().get(1).getInputRecords());

        testQuery(SQL);
    }

    private void testQuery(String sqlStr) {
        Dataset dsFromCube = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertEquals(1L, dsFromCube.count());

        Dataset dsFromSpark = NExecAndComp.querySparkSql(sqlStr);
        Assert.assertEquals(1L, dsFromSpark.count());
        String msg = SparkQueryTest.checkAnswer(dsFromCube, dsFromSpark, false);
        Assert.assertNotNull(msg);
    }

    private void testQueryUnequal(String sqlStr) {

        Dataset dsFromCube = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertEquals(1L, dsFromCube.count());

        Dataset dsFromSpark = NExecAndComp.querySparkSql(sqlStr);
        Assert.assertEquals(1L, dsFromSpark.count());
        String msg = SparkQueryTest.checkAnswer(dsFromCube, dsFromSpark, false);
        Assert.assertNotNull(msg);
    }

    private String convertToSparkSQL(String sqlStr) {
        return sqlStr.replaceAll("edw\\.", "");
    }

}
