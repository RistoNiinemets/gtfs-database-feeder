<?php
class GTFS_Database_Feeder {

	protected $db, $db_data;
	public $folder;

	public $valid_files = array( 'agency', 'stops', 'routes', 'trips', 'stop_times', 'calendar', 'calendar_dates', 'fare_attributes', 'fare_rules', 'shapes', 'frequencies', 'transfers', 'feed_info' );

	public $truncated_tables = array();

	function __construct( $database, $folder = 'gtfs' ) {
		$this->db_data = $database;
		$this->folder = $folder;

		$this->test_db_connection();
	}

	public function update_tables( $skip_tables = array() ) {
		$files = $this->get_files();

		foreach( $files as $table ) {
			if( in_array( $table, $skip_tables ) )
				continue;

			if( ! $this->db_table_exists( $table ) ) {
				$create = $this->db_table_create( $table );

				if( $create !== TRUE ) {
					$this->output_error( $create );

					continue;
				}
			}

			if( ( $handle = fopen( $this->folder . '/' . $table . '.txt', 'r' ) ) !== FALSE ) {
				$row = 0;
				$keys = false;

				while( ( $data = fgetcsv( $handle, 1000, ',' ) ) !== FALSE ) {
					$row++;

					if( $row == 1 ) {
						$keys = $data;
					}
					else {
						$this->fill_table_data( $table, $keys, $data );
					}

					unset( $data );
				}

				fclose( $handle );
			}
		}
	}

	function fill_table_data( $table, $fields, $data ) {
		if( is_array( $data ) ) {
			if( ! in_array( $table, $this->truncated_tables ) ) {
				$this->db->exec( 'TRUNCATE TABLE ' . $table );

				$this->truncated_tables[] = $table;
			}

			$fields = implode( ', ', array_values( $fields ) );

			$values = array_values( $data );
			$values = array_map( 'utf8_decode', $values );
			
			$placeholders = implode( ',', array_fill( 0, count( $values ), '?' ) );

			try {
				$query = $this->db->prepare( 'INSERT INTO '. $table .' ('. $fields .') VALUES ('. $placeholders .')' );
				$result = $query->execute( $values );
			}
			catch( PDOException $e ) {
				die( $e->getMessage() );
			}
		}
	}

	function get_files() {
		$raw_files = glob( $this->folder . '/*.txt' );
		$files = array();

		if( is_array( $raw_files ) && ! empty( $raw_files ) ) {
			foreach( $raw_files as $full_name ) {
				$file = str_ireplace( '.txt', '', $full_name );
				$file = str_ireplace( $this->folder . '/', '', $file );

				if( in_array( $file, $this->valid_files ) )
					$files[] = $file;
			}
		}

		return $files;
	}
	
	private function test_db_connection() {
		if( ! class_exists( 'PDO' ) ) return FALSE;

		try {
			$this->db = new PDO( 'mysql:host=' . $this->db_data->host . ';dbname=' . $this->db_data->base, $this->db_data->user, $this->db_data->pass );
		}
		catch( Exception $e ) {
			die( 'Unable to connect: ' . $e->getMessage() );
		}
	}

	private function db_table_exists( $table ) {
		try {
			$result = $this->db->query( "SELECT 1 FROM $table LIMIT 1" );
		}
		catch( Exception $e ) {
			return FALSE;
		}

		return $result !== FALSE;
	}

	private function db_table_create( $table ) {
		$varchar255 = 'varchar(255) COLLATE utf8_bin';
		$varchar50 = 'varchar(50) COLLATE utf8_bin';
		$varchar30 = 'varchar(30) COLLATE utf8_bin';
		$varchar2 = 'varchar(2) COLLATE utf8_bin';
		$decimal96 = 'decimal(9,6) COLLATE utf8_bin';
		$decimal915 = 'decimal(9,15) COLLATE utf8_bin';
		$decimal42 = 'decimal(4,2) COLLATE utf8_bin';
		$int2 = 'int(2) COLLATE utf8_bin';
		$int11 = 'int(11) COLLATE utf8_bin';
		$engine = ' ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;';

		if( $table == 'agency' ) {
			$cmd = 
			'CREATE TABLE `agency` (
				`agency_id` int(11) NOT NULL,
				`agency_name` '. $varchar255 .' NOT NULL,
				`agency_url` '. $varchar255 .' NOT NULL,
				`agency_timezone` '. $varchar50 .' NOT NULL,
				`agency_phone` '. $varchar30 .' DEFAULT NULL,
				`agency_lang` '. $varchar2 .' DEFAULT NULL,
				`agency_fare_url` '. $varchar255 .' DEFAULT NULL,
				PRIMARY KEY (`agency_id`)
			)' . $engine;
		}
		elseif( $table == 'stops' ) {
			$cmd = 
			'CREATE TABLE `stops` (
				`stop_id` int(11) NOT NULL,
				`stop_code` '. $varchar50 .' DEFAULT NULL,
				`stop_desc` '. $varchar255 .' DEFAULT NULL,
				`stop_name` '. $varchar255 .' NOT NULL,
				`alias` '. $varchar255 .' DEFAULT NULL,
				`stop_area` '. $varchar50 .' DEFAULT NULL,
				`stop_lat` '. $varchar50 .' DEFAULT NULL,
				`stop_lon` '. $varchar50 .' DEFAULT NULL,
				`zone_id` '. $varchar30 .' DEFAULT NULL,
				`stop_url` '. $varchar255 .' DEFAULT NULL,
				`location_type` '. $int2 .' DEFAULT NULL,
				`parent_station` '. $int11 .' DEFAULT NULL,
				`stop_timezone` '. $varchar50 .' DEFAULT NULL,
				`wheelchair_boarding` '. $int2 .' DEFAULT NULL,
				PRIMARY KEY (`stop_id`)
			)' . $engine;
		}
		elseif( $table == 'routes' ) {
			$cmd = 
			'CREATE TABLE `routes` (
				`route_id` '.$varchar50.' NOT NULL,
				`agency_id` '. $int11 .' DEFAULT NULL,
				`route_short_name` '. $varchar255 .' NOT NULL,
				`route_long_name` '. $varchar255 .' NOT NULL,
				`route_desc` '. $varchar255 .' DEFAULT NULL,
				`route_type` '. $int2 .' DEFAULT NULL,
				`route_url` '. $varchar255 .' DEFAULT NULL,
				`competent_authority` '. $varchar255 .' DEFAULT NULL,
				`route_color` '. $varchar255 .' DEFAULT NULL,
				`route_text_color` '. $varchar30 .' DEFAULT NULL,
				PRIMARY KEY (`route_id`)
			)' . $engine;
		}
		elseif( $table == 'trips' ) {
			$cmd = 
			'CREATE TABLE `trips` (
				`route_id` '. $varchar50 .' NOT NULL,
				`service_id` '. $int11 .' NOT NULL,
				`trip_id` '. $int11 .' NOT NULL,
				`trip_headsign` '. $varchar255 .' DEFAULT NULL,
				`trip_long_name` '. $varchar255 .' DEFAULT NULL,
				`trip_short_name` '. $varchar255 .' DEFAULT NULL,
				`direction_code` '. $int11 .' DEFAULT NULL,
				`block_id` '. $int11 .' DEFAULT NULL,
				`shape_id` '. $int11 .' DEFAULT NULL,
				`wheelchair_accessible` '. $int2 .' DEFAULT NULL,
				`bikes_allowed` '. $int2 .' DEFAULT NULL,
				PRIMARY KEY (`trip_id`)
			)' . $engine;
		}
		elseif( $table == 'stop_times' ) {
			$cmd = 
			'CREATE TABLE `stop_times` (
				`trip_id` int(11) NOT NULL,
				`arrival_time` '. $varchar30 .' NOT NULL,
				`departure_time` '. $varchar30 .' NOT NULL,
				`stop_id` '. $int11 .' NOT NULL,
				`stop_sequence` '. $int11 .' NOT NULL,
				`stop_headsign` '. $varchar255 .' DEFAULT NULL,
				`pickup_type` '. $int2 .' DEFAULT "0",
				`drop_off_type` '. $int2 .' DEFAULT "0",
				`shape_dist_traveled` '. $decimal42 .' DEFAULT NULL
			)' . $engine;
		}
		elseif( $table == 'calendar' ) {
			$cmd = 
			'CREATE TABLE `calendar` (
				`service_id` int(11) NOT NULL,
				`monday` '. $int2 .' NOT NULL,
				`tuesday` '. $int2 .' NOT NULL,
				`wednesday` '. $int2 .' NOT NULL,
				`thursday` '. $int2 .' NOT NULL,
				`friday` '. $int2 .' NOT NULL,
				`saturday` '. $int2 .' NOT NULL,
				`sunday` '. $int2 .' NOT NULL,
				`start_date` '.$varchar30.' NOT NULL,
				`end_date` '.$varchar30.' NOT NULL,
				PRIMARY KEY (`service_id`)
			)' . $engine;
		}
		elseif( $table == 'calendar_dates' ) {
			$cmd = 
			'CREATE TABLE `calendar_dates` (
				`service_id` int(11) NOT NULL,
				`date` '.$varchar30.' NOT NULL,
				`exception_type` '. $int2 .' NOT NULL
			)' . $engine;
		}
		elseif( $table == 'feed_info' ) {
			$cmd = 
			'CREATE TABLE `feed_info` (
				`feed_publisher_name` '. $varchar255 .' NOT NULL,
				`feed_publisher_url` '. $varchar255 .' NOT NULL,
				`feed_lang` '. $varchar30 .' NOT NULL
			)' . $engine;
		}

		if( isset( $cmd ) ) {
			try {
				$this->db->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
				$this->db->exec( $cmd );
			}
			catch( PDOException $e ) {
				return $e->getMessage();
			}
		}

		return TRUE;
	}

	private function output_error( $message ) {
		echo '<p style="border: 1px solid #f00; padding: 5px; margin: 5px 0;">' . $message . '</p>';
	}
}
