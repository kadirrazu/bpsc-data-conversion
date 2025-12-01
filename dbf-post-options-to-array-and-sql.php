<?php

// ---------------- CONFIG ----------------
$dbf_file = __DIR__ . '\file-to-convert\bcs_option44.DBF';

$cadres_table = "cadres";
$posts_table  = "posts";

$select_fields_cadres = [
    'OPT_CODE', 'OPT_DESC', 'P_NAME', 'CADRE'
];

$select_fields_posts = [
    'OPT_CODE', 'POST_NO', 'MERIT', 'FF_QUOTA', 'EM_QUOTA', 'PHC_QUOTA'
];

$field_map_cadres = [
    'OPT_CODE' => 'cadre_code',
    'OPT_DESC' => 'cadre_abbr',
    'P_NAME'   => 'cadre_name',
    'CADRE'    => 'cadre_type',
];

$field_map_posts = [
    'OPT_CODE'  => ['cadre_code'],
    'POST_NO'   => ['total_post', 'total_post_left'],
    'MERIT'     => ['mq_post', 'mq_post_left'],
    'FF_QUOTA'  => ['cff_post', 'cff_post_left'],
    'EM_QUOTA'  => ['em_post', 'em_post_left'],
    'PHC_QUOTA' => ['phc_post', 'phc_post_left'],
];

$encoding = 'CP1252';

$cadres_sql_out_file      = __DIR__ . '/conversion-output/dbf-to-file/output_cadres_sql_bcs44.sql';
$posts_sql_out_file       = __DIR__ . '/conversion-output/dbf-to-file/output_posts_sql_bcs44.sql';
$cadres_php_array_file    = __DIR__ . '/conversion-output/dbf-to-file/output_cadres_array_bcs44.php';
$posts_php_array_file     = __DIR__ . '/conversion-output/dbf-to-file/output_posts_array_bcs44.php';


// ---------------- FUNCTIONS ----------------

function parse_dbf_file(string $path, string $encoding) : array {

    if (!file_exists($path)) {
        throw new RuntimeException("DBF not found: $path");
    }

    $fp = fopen($path, 'rb');
    if (!$fp) throw new RuntimeException("Cannot open DBF");

    // header
    $header = fread($fp, 32);
    $num_records   = unpack("V", substr($header, 4, 4))[1];
    $header_length = unpack("v", substr($header, 8, 2))[1];
    $record_length = unpack("v", substr($header, 10, 2))[1];

    // fields
    $fields = [];
    fseek($fp, 32);

    while (true) {
        $desc = fread($fp, 32);
        if (strlen($desc) === 0) break;
        if (ord($desc[0]) === 0x0D) break;

        $fields[] = [
            'name'   => rtrim(substr($desc, 0, 11), "\x00 "),
            'type'   => $desc[11],
            'length' => ord($desc[16]),
        ];
    }

    fseek($fp, $header_length);

    $rows = [];

    for ($i = 0; $i < $num_records; $i++) {

        $raw = fread($fp, $record_length);
        if (!$raw) break;

        if ($raw[0] === '*') continue; // deleted record

        $pos = 1;
        $row = [];

        foreach ($fields as $f) {

            $val = rtrim(substr($raw, $pos, $f['length']), "\x00 \t\r\n");

            if ($encoding !== 'UTF-8') {
                $converted = @iconv($encoding, 'UTF-8//IGNORE', $val);
                if ($converted !== false) $val = $converted;
            }

            $row[$f['name']] = trim($val);
            $pos += $f['length'];
        }

        $rows[] = $row;
    }

    fclose($fp);
    return $rows;
}

function map_fields_simple(array $rows, array $map) : array {
    $out = [];
    foreach ($rows as $r) {
        $item = [];
        foreach ($map as $from => $to) {

            $val = trim($r[$from] ?? '') ?: null;

            // Convert cadre_code to integer
            if ($to === 'cadre_code') {
                $item[$to] = $val !== null ? (int)$val : null;
            } else {
                $item[$to] = $val;
            }
        }
        $out[] = $item;
    }
    return $out;
}


function map_fields_multi(array $rows, array $map) : array {

    $out = [];

    foreach ($rows as $r) {

        $item = [];

        foreach ($map as $from => $targets) {

            $value = trim($r[$from] ?? '');

            foreach ($targets as $field) {
                $item[$field] = $value;
            }
        }

        // convert numeric fields safely
        $numeric_fields = [
            'total_post','total_post_left',
            'mq_post','mq_post_left',
            'cff_post','cff_post_left',
            'em_post','em_post_left',
            'phc_post','phc_post_left'
        ];

        foreach ($numeric_fields as $nf) {

            $val = trim((string)($item[$nf] ?? ''));

            if ($val === '' || $val === null) {
                $item[$nf] = 0;
            } else {
                $item[$nf] = (int)$val;
            }
        }

        $out[] = $item;
    }

    return $out;
}

function generate_sql_inserts($table, $rows) {
    $sql = "";
    foreach ($rows as $r) {
        $cols = "`" . implode("`,`", array_keys($r)) . "`";

        $vals = array_map(function($v) {
            return $v === null ? "NULL" : "'" . str_replace("'", "''", $v) . "'";
        }, array_values($r));

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

    echo "Parsing DBF...<br><br>";
    $raw = parse_dbf_file($dbf_file, $encoding);

    echo "Mapping cadres...<br><br>";
    $cadres = map_fields_simple($raw, $field_map_cadres);

    echo "Mapping posts...<br><br>";
    $posts = map_fields_multi($raw, $field_map_posts);

    echo "Writing SQL...<br><br>";
    file_put_contents($cadres_sql_out_file, generate_sql_inserts($cadres_table, $cadres));
    file_put_contents($posts_sql_out_file, generate_sql_inserts($posts_table, $posts));

    echo "Writing PHP arrays...<br><br>";
    write_php_array_file($cadres_php_array_file, $cadres);
    write_php_array_file($posts_php_array_file, $posts);

    echo "Done.<br><br>";

} catch (Exception $e) {
    echo "ERROR: " . $e->getMessage();
}

