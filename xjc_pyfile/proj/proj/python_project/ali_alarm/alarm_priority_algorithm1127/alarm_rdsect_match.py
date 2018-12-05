import psycopg2


class Content:
    sql_out = """
    SELECT
    	ki.alarm_id,
    	ki.node_id,
    	ki.roadsect_id,
    	ST_LineSubstring (
    		ST_Reverse (ki.geom),
    		0,
    		50 / st_length (st_transform(ki.geom, 3857))
    	) geom
    FROM
    	(
    		SELECT
    			kb.alarm_id,
    			kb.node_id,
    			(
    				SELECT
    					ptr.roadsect_id
    				FROM
    					(
    						SELECT
    							*
    						FROM
    							dblink (
    								'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    								'select roadsect_id,geom,down_nodeid,up_nodeid from pe_tobj_roadsect'
    							) AS T (
    								roadsect_id VARCHAR (50),
    								geom geometry,
    								down_nodeid VARCHAR (50),
    								up_nodeid VARCHAR (50)
    							)
    					) ptr,
    					(
    						SELECT
    							up_nodeid,
    							down_nodeid
    						FROM
    							(
    								SELECT
    									*
    								FROM
    									dblink (
    										'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    										'select roadsect_id,geom,down_nodeid,up_nodeid from pe_tobj_roadsect'
    									) AS T (
    										roadsect_id VARCHAR (50),
    										geom geometry,
    										down_nodeid VARCHAR (50),
    										up_nodeid VARCHAR (50)
    									)
    							) pe_tobj_roadsect
    						WHERE
    							roadsect_id = kb.roadsect_id
    					) K
    				WHERE
    					ptr.up_nodeid = K .down_nodeid
    				AND ptr.down_nodeid = K .up_nodeid
    			),
    			(
    				SELECT
    					ptr.geom
    				FROM
    					(
    						SELECT
    							*
    						FROM
    							dblink (
    								'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    								'select roadsect_id,geom,down_nodeid,up_nodeid from pe_tobj_roadsect'
    							) AS T (
    								roadsect_id VARCHAR (50),
    								geom geometry,
    								down_nodeid VARCHAR (50),
    								up_nodeid VARCHAR (50)
    							)
    					) ptr,
    					(
    						SELECT
    							up_nodeid,
    							down_nodeid
    						FROM
    							(
    								SELECT
    									*
    								FROM
    									dblink (
    										'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    										'select roadsect_id,geom,down_nodeid,up_nodeid from pe_tobj_roadsect'
    									) AS T (
    										roadsect_id VARCHAR (50),
    										geom geometry,
    										down_nodeid VARCHAR (50),
    										up_nodeid VARCHAR (50)
    									)
    							) pe_tobj_roadsect
    						WHERE
    							roadsect_id = kb.roadsect_id
    					) K
    				WHERE
    					ptr.up_nodeid = K .down_nodeid
    				AND ptr.down_nodeid = K .up_nodeid
    			)
    		FROM
    			(
    				SELECT
    					kq.*, (
    						SELECT
    							gir.inter_id node_id
    						FROM
    							disposal_alarm_data dad,
    							gaode_inter_rel gir
    						WHERE
    							alarm_id = '{0}'
    						AND dad.t_angle<>-1 
    						AND gir.gaode_id = dad.inter_id
    					),
    					'{0}' alarm_id
    				FROM
    					(
    						SELECT
    							ku.*, ABS (ku.dir - ka.dir) difference_value
    						FROM
    							(
    								SELECT
    									kr.roadsect_id,
    									CEIL (
    										ST_Azimuth (kr.down_geom, kr.up_geom) / (2 * pi()) * 360
    									) :: INTEGER AS dir
    								FROM
    									(
    										SELECT
    											pr.roadsect_id,
    											pr.geom,
    											pn.geom up_geom,
    											(
    												SELECT
    													pn1.geom
    												FROM
    													(
    														SELECT
    															*
    														FROM
    															dblink (
    																'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    																'select node_id,geom from pe_tobj_node'
    															) AS T (
    																node_id VARCHAR (50),
    																geom geometry
    															)
    													) pn1
    												WHERE
    													pn1.node_id = pr.down_nodeid
    											) down_geom
    										FROM
    											(
    												SELECT
    													*
    												FROM
    													dblink (
    														'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    														'select roadsect_id,geom,down_nodeid,up_nodeid from pe_tobj_roadsect'
    													) AS T (
    														roadsect_id VARCHAR (50),
    														geom geometry,
    														down_nodeid VARCHAR (50),
    														up_nodeid VARCHAR (50)
    													)
    											) pr,
    											(
    												SELECT
    													*
    												FROM
    													dblink (
    														'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    														'select node_id,geom from pe_tobj_node'
    													) AS T (
    														node_id VARCHAR (50),
    														geom geometry
    													)
    											) pn
    										WHERE
    											down_nodeid = (
    												SELECT
    													gir.inter_id node_id
    												FROM
    													disposal_alarm_data dad,
    													gaode_inter_rel gir
    												WHERE
    													alarm_id = '{0}'
    												AND dad.t_angle<>-1
    												AND gir.gaode_id = dad.inter_id
    											)
    										AND pr.up_nodeid = pn.node_id
    									) kr
    							) ku,
    							(
    								SELECT
    									dad.alarm_id,
    									dad.t_angle dir,
    									gir.inter_id node_id
    								FROM
    									disposal_alarm_data dad,
    									gaode_inter_rel gir
    								WHERE
    									alarm_id = '{0}'
    								AND dad.t_angle<>-1
    								AND gir.gaode_id = dad.inter_id
    							) ka
    						ORDER BY
    							ku.dir
    					) kq
    				WHERE
    					difference_value < 10
    				OR difference_value > 340
    				ORDER BY
    					difference_value
    				LIMIT 1
    			) kb
    	) ki"""

    # 进口道报警sql语句
    sql_in = """
    SELECT
    	kb.alarm_id,
    	kb.node_id,
    	kb.roadsect_id,
    	kb.geom
    FROM
    	(
    		SELECT
    			kq.*, (
    				SELECT
    					gir.inter_id node_id
    				FROM
    					disposal_alarm_data dad,
    					gaode_inter_rel gir
    				WHERE
    					alarm_id = '{0}'
    				AND dad.f_angle<>-1 
    				AND gir.gaode_id = dad.inter_id
    			),
    			'{0}' alarm_id
    		FROM
    			(
    				SELECT
    					ku.*, ABS (ku.dir - ka.dir) difference_value
    				FROM
    					(
    						SELECT
    							kr.roadsect_id,
    							ST_LineSubstring (
    								kr.geom,
    								0,
    								50 / st_length (st_transform(kr.geom, 3857))
    							) geom,
    							CEIL (
    								ST_Azimuth (kr.down_geom, kr.up_geom) / (2 * pi()) * 360
    							) :: INTEGER AS dir
    						FROM
    							(
    								SELECT
    									pr.roadsect_id,
    									pr.geom,
    									pn.geom up_geom,
    									(
    										SELECT
    											pn1.geom
    										FROM
    											(
    												SELECT
    													*
    												FROM
    													dblink (
    														'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    														'select node_id,geom from pe_tobj_node'
    													) AS T (
    														node_id VARCHAR (50),
    														geom geometry
    													)
    											) pn1
    										WHERE
    											pn1.node_id = pr.down_nodeid
    									) down_geom
    								FROM
    									(
    										SELECT
    											*
    										FROM
    											dblink (
    												'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    												'select roadsect_id,geom,down_nodeid,up_nodeid from pe_tobj_roadsect'
    											) AS T (
    												roadsect_id VARCHAR (50),
    												geom geometry,
    												down_nodeid VARCHAR (50),
    												up_nodeid VARCHAR (50)
    											)
    									) pr,
    									(
    										SELECT
    											*
    										FROM
    											dblink (
    												'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
    												'select node_id,geom from pe_tobj_node'
    											) AS T (
    												node_id VARCHAR (50),
    												geom geometry
    											)
    									) pn
    								WHERE
    									down_nodeid = (
    										SELECT
    											gir.inter_id node_id
    										FROM
    											disposal_alarm_data dad,
    											gaode_inter_rel gir
    										WHERE
    											alarm_id = '{0}'
    										AND dad.f_angle<>-1 
    										AND gir.gaode_id = dad.inter_id
    									)
    								AND pr.up_nodeid = pn.node_id
    							) kr
    					) ku,
    					(
    						SELECT
    							dad.alarm_id,
    							dad.f_angle dir,
    							gir.inter_id node_id
    						FROM
    							disposal_alarm_data dad,
    							gaode_inter_rel gir
    						WHERE
    							alarm_id = '{0}'
    						AND dad.f_angle<>-1
    						AND gir.gaode_id = dad.inter_id
    					) ka
    				ORDER BY
    					ku.dir
    			) kq
    		WHERE
    			difference_value < 10
    		OR difference_value > 340
    		ORDER BY
    			difference_value
    		LIMIT 1
    	) kb"""

def alarm_rdsect_match(alarm_id):
    demo_pg_inf = {'database': "signal_specialist", 'user': "postgres", 'password': "postgres",
                   'host': "192.168.20.46", 'port': "5432"}
    pp = demo_pg_inf
    try:
        db = psycopg2.connect(database=pp['database'], user=pp['user'], password=pp['password'], host=pp['host'],
                              port=pp['port'])
    except Exception as e:
        print('database connect failed,the error is:', e)

    cr = db.cursor()
    sql1 = Content.sql_in.format(alarm_id)
    sql2 = Content.sql_out.format(alarm_id)
    cr.execute(sql1)
    x = cr.fetchall()
    cr.execute(sql2)
    y = cr.fetchall()
    if x == []:
        if y == []:
            data = None
        else:
            data = y
    else:
        data = x

    cr.close()
    db.close()
    print(data)
    return data


if __name__ == '__main__':
    alarm_id = input('please in put the alarm_id:')
    data = alarm_rdsect_match(alarm_id)
    print(data)
