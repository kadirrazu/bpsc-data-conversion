<?php

/***********************************
* CONFIGURATION SECTION
***********************************/

//Input DBF File
$dbf_file = __DIR__ . '\file-to-convert\reg_all_bcs48.DBF';

//Output SQL-INSERT File
$sql_output_file = __DIR__ . '/conversion-output/sql_reg_all_bcs48.sql';
$php_array_output_file = __DIR__ . '/conversion-output/array_reg_all_bcs48.php';

//Encoding of DBF File
$encoding = 'CP1252';

//Table name
$table_name = "registrations";

//Fields of DBF File
$select_fields = [
    'USER', 'REG', 'NAME', 'SEX', 'DOB', 'B_DATE', 'DIST_CODE', 'DIST_NAME', 
	'B_SUBJECT','G_INSTITUT', 'G_INSTITU2', 'G_YEAR', 'DIV_CODE', 'DIV_NAME',
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
    'DIST_NAME' => 'district_name',
    'B_SUBJECT' => 'b_subject',
    'G_INSTITUT' => 'g_inst_code',
    'G_INSTITU2' => 'g_inst_name',
    'G_YEAR' => 'graduation_year',
    'DIV_CODE' => 'division_code',
	'DIV_NAME' => 'division_name',
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
