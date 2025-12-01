<?php
/**
 * DBF FIELD INSPECTOR — PURE PHP
 * Lists field names, types, lengths
 */

$path = "D:/xampp/htdocs/data-conversion/file-to-convert/tab_fin_bcs40.dbf";

if (!file_exists($path)) {
    die("File not found.\n");
}

$fp = fopen($path, 'rb');
if (!$fp) {
    die("Cannot open file.\n");
}

$header = fread($fp, 32);
$header_length = unpack("v", substr($header, 8, 2))[1];

fseek($fp, 32);

echo "=== DBF FIELD LIST ===\n\n";

while (true) {
    $field = fread($fp, 32);
    if (strlen($field) < 32) break;

    // Field terminator
    if ($field[0] == chr(0x0D)) break;

    $name  = rtrim(substr($field, 0, 11), "\x00 ");
    $type  = $field[11];
    $len   = ord($field[16]);
    $dec   = ord($field[17]);

    echo "Field: $name | Type: $type | Length: $len | Decimals: $dec\n";
}

echo "\nDONE.\n";