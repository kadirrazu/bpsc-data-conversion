<?php

/***********************************
* CONFIGURATION SECTION
***********************************/

//Input DBF File
$dbf_file = __DIR__ . '\..\..\file-io\file-to-convert\for-statistics\preli_passed_bcs44_stat.DBF';

//Output SQL-INSERT File
$sql_output_file = __DIR__ . '/../../file-io/file-output/dbf-to-file/bcs-statistics/bcs44/sql_preli_passed_bcs44_stat.sql';

$php_array_output_file = __DIR__ . '/../../file-io/file-output/dbf-to-file/bcs-statistics/bcs44/array_preli_passed_bcs44_stat.php';

//Encoding of DBF File
$encoding = 'CP1252';

//Table name
$table_name = "preli_passed_44";

//Fields of DBF File
$select_fields = [
    'USER', 'REG', 'NAME', 'SEX', 'DOB', 'B_DATE', 'DIST_CODE',
	'B_SUBJECT', 'GINS_CODE', 'GINS_NAME', 'CAT', 'CADRE_TYPE',
];

//Mapping of DBF File Fields to My-SQL Table Columns
$field_map = [
    'USER' => 'user_id',
	'REG' => 'reg',
    'NAME' => 'name',
	'SEX' => 'gender',
    'DOB' => 'dob',
    'B_DATE' => 'dob_ddmmyyyy',
    'DIST_CODE' => 'district_code',
    'B_SUBJECT' => 'b_subject',
    'GINS_CODE' => 'g_inst_code',
    'GINS_NAME' => 'g_inst_name',
    'CAT' => 'cadre_category',
    'CADRE_TYPE' => 'cadre_type',
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

            $rawVal = substr($raw, $pos, $f['length']);

            switch ($f['type']) {

                // Character fields
                case 'C':
                    $val = rtrim($rawVal, "\x00 \t\r\n");
                    if ($encoding !== 'UTF-8') {
                        $converted = @iconv($encoding, 'UTF-8//IGNORE', $val);
                        if ($converted !== false) $val = $converted;
                    }
                    $row[$f['name']] = trim($val);
                    break;

                // Numeric fields (ASCII)
                case 'N':
                case 'F':
                    $val = trim($rawVal);
                    $row[$f['name']] = ($val === '') ? null : $val;
                    break;

                // Integer fields (BINARY!)
                case 'I':
                    $row[$f['name']] = unpack('V', $rawVal)[1]; // unsigned little-endian 32-bit
                    break;

                // Currency
                case 'Y':
                    $row[$f['name']] = unpack('q', $rawVal)[1] / 10000;
                    break;

                default:
                    $row[$f['name']] = trim($rawVal);
            }

            $pos += $f['length'];

        }

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

        $vals = array_map(function($v) {
            if ($v === null) return "NULL";
            if (is_int($v) || is_float($v)) return $v;
            return "'" . str_replace("'", "''", $v) . "'";
        }, array_values($r));

        $sql .= "INSERT INTO `$table` ($cols) VALUES (" . implode(",", $vals) . ");\n";
    }

    return $sql;

} //Enf of function 'generate_sql_inserts()'


function generate_sql_inserts_batch(string $table, array $rows, int $batchSize = 1000) : string {

    if (empty($rows)) return '';

    $sql = '';
    $columns = array_keys($rows[0]);
    $colSql = "`" . implode("`,`", $columns) . "`";

    $batch = [];
    $count = 0;

    foreach( $rows as $row )
    {

        $values = [];

        foreach ($columns as $col) 
        {
            $v = $row[$col] ?? null;

            if ($v === null) {
                $values[] = "NULL";
            } elseif (is_int($v) || is_float($v)) {
                $values[] = $v;
            } else {
                $values[] = "'" . str_replace("'", "''", $v) . "'";
            }
        }

        $batch[] = "(" . implode(",", $values) . ")";
        $count++;

        if ($count % $batchSize === 0) {
            $sql .= "INSERT INTO `$table` ($colSql) VALUES\n"
                 . implode(",\n", $batch)
                 . ";\n\n";
            $batch = [];
        }

    }

    // Flush remaining rows
    if (!empty($batch)) {
        $sql .= "INSERT INTO `$table` ($colSql) VALUES\n"
             . implode(",\n", $batch)
             . ";\n\n";
    }

    return $sql;
}


function write_php_array_file( $path, $rows )
{

    $content = "<?php\nreturn " . var_export($rows, true) . ";\n";

    file_put_contents($path, $content);

} //Enf of function 'write_php_array_file()'


function dbf_ddmmyy_to_ymd(?string $value) : ?string
{
    $value = trim((string)$value);

    // Must be exactly 6 digits
    if (!preg_match('/^\d{6}$/', $value)) {
        return null;
    }

    $dd = substr($value, 0, 2);
    $mm = substr($value, 2, 2);
    $yy = substr($value, 4, 2);

    // Decide century (DBF rule)
    // 00–29 → 2000–2029
    // 30–99 → 1930–1999
    $year = ((int)$yy <= 29) ? (2000 + (int)$yy) : (1900 + (int)$yy);

    // Validate date
    if (!checkdate((int)$mm, (int)$dd, $year)) {
        return null;
    }

    return sprintf('%04d-%02d-%02d', $year, $mm, $dd);
	
} //Enf of function 'dbf_ddmmyy_to_ymd()'


//---------------- RUN ----------------

try {

    echo "Parsing DBF...<br><br>";
	
    $raw = parse_dbf_file($dbf_file, $select_fields, $encoding);

    echo "Mapping fields...<br><br>";
	
    $mapped = map_fields($raw, $field_map);

    //Convert some field to integer
    $int_fields = [
        'reg',
        'gender',
        'district_code',
        'b_subject',
        'g_inst_code',
        'cadre_type',
    ];

    //Cast some fields to INTEGER
	foreach ($mapped as &$row) {
        foreach ($int_fields as $f) {
            if (
                isset($row[$f]) &&
                $row[$f] !== null &&
                $row[$f] !== '' &&
                is_numeric($row[$f])
            ) {
                $row[$f] = (int)$row[$f];
            } else {
                $row[$f] = null;
            }
        }
    }

    unset( $row );
	
	//Convert b_date to dob in specific format
	foreach ($mapped as &$row) {
		// Convert DDMMYY → YYYY-MM-DD
		if (!empty($row['dob_ddmmyyyy'])) {
			$row['dob'] = dbf_ddmmyy_to_ymd($row['dob_ddmmyyyy']);
		} else {
			$row['dob'] = null;
		}
	}

    unset( $row );
	
	//Set cadre category field as per cadre_type
	foreach ($mapped as &$row) {
		
		$row['cadre_category'] = NULL;
		
		if (!empty($row['cadre_type'])) {
			if( $row['cadre_type'] === 1 ){
				$row['cadre_category'] = 'GG';
			}
			else if( $row['cadre_type'] === 2 ){
				$row['cadre_category'] = 'TT';
			}
			else if( $row['cadre_type'] === 3 ){
				$row['cadre_category'] = 'GT';
			}
		}
	}

    unset( $row );

    echo "Writing SQL...<br><br>";

    file_put_contents(
        $sql_output_file,
        generate_sql_inserts_batch($table_name, $mapped, 1000)
    );

    echo "Writing PHP array...<br><br>";
	
    write_php_array_file($php_array_output_file, $mapped);

    echo "Done.<br><br>";

} catch (Exception $e) {

    echo "ERROR: " . $e->getMessage();

} //End of Try-Catch Block
