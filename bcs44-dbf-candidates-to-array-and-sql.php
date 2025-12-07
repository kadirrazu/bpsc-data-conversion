<?php

/***********************************
* CONFIGURATION SECTION
***********************************/

//Input DBF File
$dbf_file = __DIR__ . '\file-to-convert\tab_fin_bcs44.DBF';

//Output SQL-INSERT File
$sql_output_file = __DIR__ . '/conversion-output/dbf-to-file/candidates/candidates_sql_bcs44.sql';

//Output PHP-ARRAY File
$php_array_output_file = __DIR__ . '/conversion-output/dbf-to-file/candidates/candidates_array_bcs44.php';

//Encoding of DBF File
$encoding = 'CP1252';

//Table name
$table_name = "candidates";

//Fields of DBF File
$select_fields = [
    'USER', 'REG', 'NAME', 'CAT', 'DISTRICT', 'SEX',
    'MERIT_GEN', 'MERIT_TECH', 'ALLM_TECH', 'FF_STATUS', 'TRIBAL', 'PHC', 'APASS_CODE', 'TPASS_CODE',
	'has_quota', 'quota_info',
];

//Mapping of DBF File Fields to My-SQL Table Columns
$field_map = [
    'USER' => 'user_id',
	'REG' => 'reg',
    'NAME' => 'name',
    'CAT' => 'cadre_category',
    'DISTRICT' => 'district',
    'SEX' => 'gender',
    'MERIT_GEN' => 'general_merit_position',
    'MERIT_TECH' => 'technical_merit_position',
    'ALLM_TECH' => 'technical_passed_cadres',
    'APASS_CODE' => 'choice_list',
    'TPASS_CODE' => 'choice_list_tech',
	'has_quota' => 'has_quota',
	'quota_info' => 'quota_info',
];


/***********************************
* CUSTOM FUNCTIONS
***********************************/

function parse_dbf_file(string $path, array $select_fields, string $encoding) : array {

    if (!file_exists($path)) throw new RuntimeException("File not found: $path");

    $fp = fopen($path, 'rb');

    if( !$fp ) throw new RuntimeException("Cannot open DBF");

    //Header
    $header = fread($fp, 32);

    $num_records = unpack("V", substr($header, 4, 4))[1];
    $header_length = unpack("v", substr($header, 8, 2))[1];
    $record_length = unpack("v", substr($header, 10, 2))[1];

    //Read field descriptors
    $fields = [];

    fseek($fp, 32);

    while( true )
    {
        $desc = fread($fp, 32);

        if( strlen($desc) === 0 ) break;
        if( ord($desc[0]) === 0x0D ) break;

        $fields[] = [
            'name' => rtrim(substr($desc, 0, 11), "\x00 "),
            'type' => $desc[11],
            'length' => ord($desc[16]),
        ];

    }

    fseek($fp, $header_length);

    $rows = [];

    for( $i = 0; $i < $num_records; $i++ )
    {

        $raw = fread($fp, $record_length);

        if( !$raw ) break;

        if( $raw[0] === '*' ) continue;

        $pos = 1;
        $row = [];

        foreach( $fields as $f )
        {
            $val = rtrim( substr($raw, $pos, $f['length']), "\x00 \t\r\n" );

            //Encoding convert
            if( $encoding !== 'UTF-8' )
            {
                $converted = @iconv( $encoding, 'UTF-8//IGNORE', $val );
                if( $converted !== false ) $val = $converted;
            }

            $row[$f['name']] = trim($val);
            $pos += $f['length'];
        }
		
		//--- QUOTA LOGIC ---
		$ff = (int)($row['FF_STATUS'] ?? 0);
		$em = (int)($row['TRIBAL'] ?? 0);
		$phc = (int)($row['PHC'] ?? 0);

		$quota_info = [
			'CFF' => ($ff === 2 || $ff === 3),
			'EM'  => ($em === 1),
			'PHC' => ($phc === 1),
		];

		$row['has_quota'] = $quota_info['CFF'] || $quota_info['EM'] || $quota_info['PHC'];

		$row['quota_info'] = json_encode( $quota_info );
		
		//--- PARSE TECHNICAL PASSED CADRES ---
		$tech_str = $row['ALLM_TECH'] ?? '{}';

		$tech_json = [];

		if( !empty($tech_str) )
        {
			//split by comma
			$parts = explode(',', $tech_str);

			foreach( $parts as $p )
            {
				//Match "CODE(NUMBER)" pattern
				if( preg_match('/([A-Z]+)\s*\(\s*(\d+)\s*\)/i', $p, $matches) )
                {
					$code = strtoupper(trim($matches[1]));
					$value = (int)trim($matches[2]);
					$tech_json[$code] = $value;
				}
			}
		}

		//Replace field with JSON
		$row['ALLM_TECH'] = json_encode($tech_json);
		
		//Encode technical_passed_cadres as JSON object
		if( empty($tech_json) )
        {
			$row['ALLM_TECH'] = '{}';
		}
        else
        {
			$row['ALLM_TECH'] = json_encode( $tech_json );
		}
		
		//--- NORMALIZE CADRE CATEGORY ---
		$cat = strtoupper( trim($row['CAT'] ?? '') );

		//If 'T', replace with 'TT'
		if( $cat === 'T' )
        {
			$cat = 'TT';
		}
		
		if( $cat === 'GN' )
        {
			$cat = 'GG';
		}
		
		$row['CAT'] = $cat;

		//--- SET MERIT POSITIONS BASED ON CADRE ---
		if( $cat === 'GG' )
        {
			//General cadre → technical merit = NULL
			$row['MERIT_TECH'] = null;
			$row['MERIT_GEN'] = isset($row['MERIT_GEN']) ? (int)$row['MERIT_GEN'] : null;
		} 
        elseif( $cat === 'TT' )
        {
			//Technical cadre → general merit = NULL
			$row['MERIT_GEN'] = null;
			$row['MERIT_TECH'] = isset($row['MERIT_TECH']) ? (int)$row['MERIT_TECH'] : null;
		} 
        else
        {
			//Fallback: cast both
			$row['MERIT_GEN'] = isset($row['MERIT_GEN']) ? (int)$row['MERIT_GEN'] : null;
			$row['MERIT_TECH'] = isset($row['MERIT_TECH']) ? (int)$row['MERIT_TECH'] : null;
		}
		
		$gender_map = [
			'1' => 'Male',
			'2' => 'Female',
			'3' => 'Third Gender',
		];

		//Replace numeric code with text
		$gender_code = trim( (string)($row['SEX'] ?? '') );

		$row['SEX'] = $gender_map[$gender_code] ?? $gender_code; //Fallback to original if unknown

        // filter selected fields
        $filtered = [];

        foreach( $select_fields as $f )
        {
            $filtered[$f] = $row[$f] ?? null;
        }

        $rows[] = $filtered;

    } //End of FOR LOOP

    fclose($fp);

    return $rows;

} //End of function 'parse_dbf_file()'


function map_fields(array $rows, array $map) : array {

    $out = [];

    foreach( $rows as $r )
    {
        $item = [];

        foreach( $map as $from => $to )
        {
            $item[$to] = $r[$from] ?? null;
        }

        $out[] = $item;

    }

    return $out;

} //Enf of function 'map_fields()'


function generate_sql_inserts( $table, $rows )
{
    $sql = "";

    foreach( $rows as $r )
    {
        $cols = "`" . implode("`,`", array_keys($r)) . "`";

        $vals = array_map(fn($v) =>
            $v === null ? "NULL" : "'" . str_replace("'", "''", $v) . "'",
        array_values($r));

        $sql .= "INSERT INTO `$table` ($cols) VALUES (" . implode(",", $vals) . ");\n";
    }

    return $sql;

} //Enf of function 'generate_sql_inserts()'

function write_php_array_file( $path, $rows )
{

    $content = "<?php\nreturn " . var_export($rows, true) . ";\n";

    file_put_contents($path, $content);

} //Enf of function 'write_php_array_file()'


//---------------- RUN ----------------

try {

    echo "Parsing DBF...<br><br>";
    $raw = parse_dbf_file($dbf_file, $select_fields, $encoding);

    echo "Mapping fields...<br><br>";
    $mapped = map_fields($raw, $field_map);

    echo "Writing SQL...<br><br>";
    file_put_contents($sql_output_file, generate_sql_inserts($table_name, $mapped));

    echo "Writing PHP array...<br><br>";
    write_php_array_file($php_array_output_file, $mapped);

    echo "Done.<br><br>";

} catch (Exception $e) {

    echo "ERROR: " . $e->getMessage();

} //End of Try-Catch Block
