<?php
// enrich_groups_with_hovercard.php
// PHP port of enrich_groups_with_hovercard.py

function prompt($prompt) {
    echo $prompt;
    return trim(fgets(STDIN));
}

function load_json($path) {
    if (!file_exists($path)) return null;
    $data = file_get_contents($path);
    return json_decode($data, true);
}

function save_json($path, $data) {
    file_put_contents($path, json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
}

function append_jsonl($path, $obj) {
    file_put_contents($path, json_encode($obj, JSON_UNESCAPED_UNICODE) . "\n", FILE_APPEND);
}

function random_sleep($min, $max) {
    $t = mt_rand($min * 1000, $max * 1000) / 1000.0;
    echo "Sleeping for {$t} seconds...\n";
    usleep($t * 1000000);
}

// --- Paths ---
$SCRIPT_DIR = __DIR__;
$PARENT_DIR = dirname($SCRIPT_DIR);
$INPUT_FILE = "$PARENT_DIR/output/groups_php.jsonl";
$OUTPUT_FILE = "$PARENT_DIR/output/groups_enriched_php.jsonl";
$COOKIE_FILE = "$PARENT_DIR/settings/cookie.json";
$HOVERCARD_DOC_ID = "24553182484278857";
// Ask user if they want sleep mode
echo "Enable sleep mode between requests? [y/N]: ";
$sleep_mode = strtolower(trim(fgets(STDIN)));
if ($sleep_mode === 'y' || $sleep_mode === 'yes') {
    echo "Sleep mode enabled.\n";
    $SLEEP_BETWEEN_REQUESTS = [1.5, 4.0];
} else {
    echo "Sleep mode disabled.\n";
    $SLEEP_BETWEEN_REQUESTS = [0.1, 0.2];
}

// --- Proxy Mode Selection ---
echo "Choose proxy mode: [1] Nimbleway proxy [2] Proxyless\n";
$mode = prompt("Enter 1 or 2 (default 1): ");
if ($mode === "2") {
    $PROXIES = null;
    echo "Running proxyless (direct connection)...\n";
} else {
    $NIMBLE_SETTINGS_FILE = "$PARENT_DIR/settings/nimble_settings.json";
    if (file_exists($NIMBLE_SETTINGS_FILE)) {
        $nimble_settings = load_json($NIMBLE_SETTINGS_FILE);
        $NIMBLE_USERNAME = $nimble_settings["username"] ?? null;
        $NIMBLE_PASSWORD = $nimble_settings["password"] ?? null;
        $NIMBLE_HOST = $nimble_settings["host"] ?? "ip.nimbleway.com";
        $NIMBLE_PORT = $nimble_settings["port"] ?? "7000";
        $NIMBLE_PROXY = "http://$NIMBLE_USERNAME:$NIMBLE_PASSWORD@$NIMBLE_HOST:$NIMBLE_PORT";
        $PROXIES = $NIMBLE_PROXY;
        echo "Using Nimbleway proxy: $NIMBLE_PROXY\n";
    } else {
        $PROXIES = null;
        echo "Nimble settings not found, running proxyless.\n";
    }
}

// --- Load cookies ---
$COOKIES = load_json($COOKIE_FILE);
if (!$COOKIES || !isset($COOKIES["c_user"])) {
    exit("Cookie file missing or invalid.\n");
}

function load_enriched_ids($OUTPUT_FILE) {
    $enriched_ids = [];
    if (file_exists($OUTPUT_FILE)) {
        $lines = file($OUTPUT_FILE, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        foreach ($lines as $line) {
            $group = json_decode($line, true);
            if (isset($group["id"])) {
                $enriched_ids[$group["id"]] = true;
            }
        }
    }
    return $enriched_ids;
}

function make_hovercard_variables($group_id) {
    return json_encode([
        "actionBarRenderLocation" => "WWW_COMET_HOVERCARD",
        "context" => "DEFAULT",
        "entityID" => $group_id,
        "scale" => 1,
        "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider" => false
    ]);
}

function extract_hovercard_fields($resp) {
    try {
        $group = $resp["data"]["node"]["comet_hovercard_renderer"]["group"];
        return [
            "name" => $group["name"] ?? null,
            "url" => $group["url"] ?? null,
            "member_count" => $group["group_member_profiles"]["formatted_count_text"] ?? null,
            "privacy" => $group["privacy_info"]["title"]["text"] ?? null,
        ];
    } catch (Exception $e) {
        echo "Error extracting hovercard fields: {$e->getMessage()}\n";
        return [];
    }
}

// --- HEADERS (update values as needed) ---
$HEADERS = [
    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept: */*",
    "Accept-Encoding: gzip, deflate",
    "Accept-Language: en-US,en;q=0.5",
    "Content-Type: application/x-www-form-urlencoded",
    "Origin: https://www.facebook.com",
    "Referer: https://www.facebook.com/groups/search/groups_home/?q=Dallas%2C%20TX",
    "Sec-Fetch-Site: same-origin",
    "Sec-Fetch-Mode: cors",
    "Sec-Fetch-Dest: empty",
    "X-FB-Friendly-Name: CometHovercardQueryRendererQuery",
    "x-fb-lsd: 84EMFCpbSO1IO3zao5ViuJ",
    "x-asbd-id: 359341",
];

// --- Main enrichment loop ---
if (!file_exists($INPUT_FILE)) {
    exit("Input file $INPUT_FILE not found.\n");
}
$lines = file($INPUT_FILE, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
$groups = [];
foreach ($lines as $line) {
    $groups[] = json_decode($line, true);
}
$enriched_ids = load_enriched_ids($OUTPUT_FILE);
echo "Skipping ".count($enriched_ids)." groups already enriched.\n";
$enriched = [];
foreach ($groups as $idx => $group) {
    if (isset($enriched_ids[$group["id"]])) continue;
    $group_id = $group["id"];
    $variables = make_hovercard_variables($group_id);
    $data = [
        "av" => $COOKIES["c_user"],
        "__aaid" => "0",
        "__user" => $COOKIES["c_user"],
        "__a" => "1",
        "__req" => "1q",
        "__hs" => "20285.HYP:comet_pkg.2.1...0",
        "dpr" => "1",
        "__ccg" => "EXCELLENT",
        "__rev" => "1024785322",
        "__s" => "19rbhi:cf27t6:tmmyxk",
        "__hsi" => "7527498299079297119",
        "__dyn" => "7xeUjGU5a5Q1ryaxG4Vp41twWwIxu13wFwkUKewSwAyUco2qwJyE2OwpUe8hwaG0Z82_CxS320qa321Rwwwqo462mcw5Mx62G5Usw9m1YwBgK7o6C0Mo4G17yovwRwlE-U2exi4UaEW2G1jwUBwJK14xm1Wxfxmu3W3y261eBx_wHwfC2-awLyESE2KwkQ0z8c86-bwHwKG4UrwFg2fwxyo6J0qo4e4UO2m3G1eKufxamEbbxG1fBG2-2K0E8461wweW2K3abxG",
        "__csr" => "gB1P4gBbNY55N4Yp8xauCzlf6Olsj3b6nOHYCIGcLWky9ncojF9OlQG9vEAZYhqWaRlFai-IyBGFKAGi_VXlbAHuVaX-AQqXC-EC9XB_jVGGHCGfgmmh5AKiaAAgCdyUvzFJ5mAaDy8G8x6m8xymeAKm23WxmiU8Ghbyuq2OubwDByUOEaEx7K4Uhxa78gzEaoboLG58Na2m326Kcz9VqwLwAmbwNz44Ey4U9VUd8Sq8gZe6U98O3nwLxe2Wayoc8sx26EmG1zwTwywMwLwMx6m8x6i3jxWcyUbFUa8hAwh8hw9m1Twg82VG08CgozUN0uU2dc0uibwai0VE28wd62Ne0ju4oGWa2i5oB0lE1wEdoBie1fw56BwhQmaO8wcm3G0eVw0nI8co0Ge0tl5hk025Hg03qUwc4Wxi6CEnCw4WgIwdawaG0IE1Q814qiyEIk02tC640vHg0k6gzy80odw5ADlw2UIM3dyE0Qh0q60hLw1lW4EbUiw9R1u3C0ky09Rg0h5w2W8iw0BVK0iK09Bw2zooy5o1pA0qu3O05S80-wwc81dHw5cw7Ky5w",
        "__hsdp" => "gbqsG86VUgh0xEG9azaAuqUjxsxG8x4OpzkhZi4sWLih4AikwR5gSAi4QFFEWExyy5i8xYaeMUnGGAIJWAacV5LikVaucF5yoyulGUx4mTKEFuyt8Jd8gBBbSyT5ejhahbp95yEWKWdiLpa6nhWHJcsHajJGhEQdjFbR5hj3q6hexBEb8gOF8HCAqgSZO4qagAFIFKhIG2B4jhExKahaHt5LmuXS8jBpoxck8qpF4eCjCGcF8xALc4a88GiDmVkmiAbiUH8j_qjFdrXDBkBESi4Q8IynjvkUBVGW45QKdayCF24iD8p6UCQiCdEVWgBgEp8Nby9WGlAEqnyWymp2h0i8qhaGzWh8nyFoSag8pEQicBP2Gg88lwxpWwKx2pAaS7WqwMwmroW7rg4a13grzETy6XzUBcFOI8CKVVpoTyobEvAxGVouyZ7zCm64bBcAgNla5obUOWxmEJKNDw4doZ0OwhUiF8E2-yoiwk82bg3Ew8y2QMc8vglwh4qWhB5167UZ0du4k2pyoswjAdwasxtOClyzOm6Ukx-0JU6K1BwVwaG0G898661swpo27Cw4Lxa1gwYxKh040w8i0IE1YE410Axa0nvxS0i61-wgU2gx28x6awqEO0z8do0wS0pu1Zw7Pw9i11w5mwi8ow2R81H83uw6ow5gwaS0VE1oE0Cu0lO17w",
        "__hblp" => "04sxKawUwoU2dxudG13wGy8nw2vVU6u223u1-wuEeU4y5oSmiu2OdUaefyU9833wsE1xE6m0T86i0zE4N0naw5awQwrawcq6Q1gw8uQ0qe0UEvg-1cCwb-2S2q581b8jxa0AU1a82GwcS1Mwjo1ibwp83mwYw9W15wWw8vyo21wADwdm0lm5o-0Po3nw4Cw8a3-0GE2kw8O3m4EdU1585q7EdA3O0yE3fwSwhEb8dE8E621fwa26E5q3S11w8q0PU4y6bzU-Hw9-15wOwRw5poeo1iU9o1ro3pwXwb-0jaUc84y1ECwww4dz8vgvwae11g1nV9o5G1sx6689827gyaw",
        "__sjsp" => "gbqsG86VUgh0xF5giKuqUjxsxG8x4OpzkhZi4sWLih4AikwR5ggh8jgC5qyEExkCgt5N3Ie5AGEwKqgwTAmZ5c9xa48ydqzpXuWyBW9Xbji8WmlbzfpHzUF2EuVEGZxaUiJd2VU2fA98iewh8jLp96Ad888vyA4rHBjDyE-iml2uE-E522ifi8qsMgEwyuCKVkaKbx38cjAGh2VVkKA3eGz8jG6ZafSyyoCmAEB6UCi6z2Q9kOOWCigO8DG8xLyWymp2h0i8biBzWgboS0SobE520yEcHg26gee8rK2EHkpCKUjzu1twgULg6d0mbG2qX6u0gRzQ3a0iW0tC0y86e3q14hHF6kk4obU3ox50Cg1e7apmaf9o520JU0WC0xVE1bUiw8S6V40g20cGwg40q_xS0q20R8gxGawqE0eS80KC",
        "__comet_req" => "15",
        "fb_dtsg" => "NAftck539DhlFUznehfoGAYC5PKDIIPh_3fX30jwthhEmP-sXtj9LcA:43:1750474603",
        "jazoest" => "25456",
        "lsd" => "84EMFCpbSO1IO3zao5ViuJ",
        "__spin_r" => "1024785322",
        "__spin_b" => "trunk",
        "__spin_t" => "1752632274",
        "__crn" => "comet.fbweb.CometGroupsSearchRoute",
        "fb_api_caller_class" => "RelayModern",
        "fb_api_req_friendly_name" => "CometHovercardQueryRendererQuery",
        "variables" => $variables,
        "server_timestamps" => "true",
        "doc_id" => $HOVERCARD_DOC_ID,
    ];
    $postfields = http_build_query($data);
    $ch = curl_init("https://www.facebook.com/api/graphql/");
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $postfields);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $HEADERS);
    // Set cookies
    $cookie_str = '';
    foreach ($COOKIES as $k => $v) {
        $cookie_str .= "$k=$v; ";
    }
    curl_setopt($ch, CURLOPT_COOKIE, $cookie_str);
    if ($PROXIES) {
        curl_setopt($ch, CURLOPT_PROXY, $PROXIES);
    }
    // Enable auto-decompression for gzip/deflate if possible
    curl_setopt($ch, CURLOPT_ENCODING, '');
    // Include headers in the output
    curl_setopt($ch, CURLOPT_HEADER, true);
    $resp = curl_exec($ch);
    if ($resp === false) {
        $error = curl_error($ch);
        curl_close($ch);
        echo "cURL error: $error\n";
        continue;
    }
    $header_size = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
    $headers = substr($resp, 0, $header_size);
    $body = substr($resp, $header_size);
    $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    $result = json_decode($body, true);
    if ($result === null) {
        echo "\n--- RESPONSE HEADERS ---\n$headers\n--- END HEADERS ---\n";
        echo "First 500 bytes of response:\n";
        echo substr($body, 0, 500) . "\n--- END OF RESPONSE ---\n";
        exit(1);
    }
    $hovercard = extract_hovercard_fields($result);
    $group = array_merge($group, $hovercard);
    append_jsonl($OUTPUT_FILE, $group);
    echo "[".($idx+1)."/".count($groups)."] Enriched group $group_id: ".$group["name"]."\n";
    random_sleep($SLEEP_BETWEEN_REQUESTS[0], $SLEEP_BETWEEN_REQUESTS[1]);
}
echo "Done. Enriched data written to $OUTPUT_FILE\n"; 