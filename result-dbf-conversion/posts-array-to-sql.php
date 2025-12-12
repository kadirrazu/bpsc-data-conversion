<?php

include 'file-to-convert/posts-array-raw.php';

$sqlRows = [];

foreach ($post_available as $cadre_code => $c) {

    $total_post = $c['total_post'] ?? 0;
    $mq = $c['mq'] ?? 0;
    $cff = $c['cff'] ?? 0;
    $em = $c['em'] ?? 0;
    $phc = $c['phc'] ?? 0;

    // Build row
    $sqlRows[] = sprintf(
        "(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)",
        $cadre_code,
        $total_post,
        $total_post,  // total_post_left
        $mq,
        $mq,          // mq_left
        $cff,
        $cff,         // cff_left
        $em,
        $em,          // em_left
        $phc,
        $phc          // phc_left
    );
}

$finalSQL =
"INSERT INTO `posts`
(`cadre_code`, `total_post`, `total_post_left`, `mq_post`, `mq_post_left`,
 `cff_post`, `cff_post_left`, `em_post`, `em_post_left`, `phc_post`, `phc_post_left`)
VALUES\n" . implode(",\n", $sqlRows) . ";";

$file = 'conversion-output/posts_sql.txt';
file_put_contents($file, $finalSQL);

echo "SQL export saved to $file";