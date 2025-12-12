<?PHP

// ---------------- CONFIG ----------------
$dbf_file = __DIR__ . '\file-to-convert\bcs_job_option.DBF';

$table_name = "cadres";

$select_fields = [
    'OPT_CODE','OPT_DESC','CADRE','POST_NO', 'P_NAME'
];

$field_map = [
    'OPT_CODE' => 'cadre_code',
	'CADRE' => 'cadre_type',
    'OPT_DESC' => 'cadre_abbr',
	'P_NAME' => 'cadre_name',
    'POST_NO' => 'total_post',
];

$encoding = 'CP1252';

$sql_out_file = __DIR__ . '/conversion-output/dbf-to-file/output_post_sql.sql';
$php_array_file = __DIR__ . '/conversion-output/dbf-to-file/output_post_array.php';

// ---------------- FUNCTIONS ----------------

function parse_dbf_file(string $path, array $select_fields, string $encoding) : array {

    if (!file_exists($path)) throw new RuntimeException("File not found: $path");

    $fp = fopen($path, 'rb');
    if (!$fp) throw new RuntimeException("Cannot open DBF");

    // header
    $header = fread($fp, 32);
    $num_records = unpack("V", substr($header, 4, 4))[1];
    $header_length = unpack("v", substr($header, 8, 2))[1];
    $record_length = unpack("v", substr($header, 10, 2))[1];

    // read field descriptors
    $fields = [];
    fseek($fp, 32);
    while (true) {
        $desc = fread($fp, 32);
        if (strlen($desc) === 0) break;
        if (ord($desc[0]) === 0x0D) break;

        $fields[] = [
            'name' => rtrim(substr($desc, 0, 11), "\x00 "),
            'type' => $desc[11],
            'length' => ord($desc[16]),
        ];
    }

    fseek($fp, $header_length);
    $rows = [];

    for ($i = 0; $i < $num_records; $i++) {

        $raw = fread($fp, $record_length);
        if (!$raw) break;

        if ($raw[0] === '*') continue;

        $pos = 1;
        $row = [];

        foreach ($fields as $f) {
            $val = rtrim(substr($raw, $pos, $f['length']), "\x00 \t\r\n");

            // encoding convert
            if ($encoding !== 'UTF-8') {
                $converted = @iconv($encoding, 'UTF-8//IGNORE', $val);
                if ($converted !== false) $val = $converted;
            }

            $row[$f['name']] = trim($val);
            $pos += $f['length'];
        }

        // -------------------------------

        // filter selected fields
        $filtered = [];
        foreach ($select_fields as $f) {
            $filtered[$f] = $row[$f] ?? null;
        }

        $rows[] = $filtered;
    }

    fclose($fp);
    return $rows;
}

function map_fields(array $rows, array $map) : array {
    $out = [];
    foreach ($rows as $r) {
        $item = [];
        foreach ($map as $from => $to) {
            $item[$to] = $r[$from] ?? null;
        }
        $out[] = $item;
    }
    return $out;
}

function generate_sql_inserts($table, $rows) {
    $sql = "";
    foreach ($rows as $r) {
        $cols = "`" . implode("`,`", array_keys($r)) . "`";
        $vals = array_map(fn($v) =>
            $v === null ? "NULL" : "'" . str_replace("'", "''", $v) . "'",
        array_values($r));

        $sql .= "INSERT INTO `$table` ($cols) VALUES (" . implode(",", $vals) . ");\n";
    }
    return $sql;
}

function write_php_array_file($path, $rows) {
    $content = "<?php\nreturn " . var_export($rows, true) . ";\n";
    file_put_contents($path, $content);
}

// ---------------- RUN ----------------

try {
    echo "Parsing DBF...\n";
    $raw = parse_dbf_file($dbf_file, $select_fields, $encoding);

    echo "Mapping fields...\n";
    $mapped = map_fields($raw, $field_map);

    echo "Writing SQL...\n";
    file_put_contents($sql_out_file, generate_sql_inserts($table_name, $mapped));

    echo "Writing PHP array...\n";
    write_php_array_file($php_array_file, $mapped);

    echo "Done.\n";

} catch (Exception $e) {
    echo "ERROR: " . $e->getMessage();
}
