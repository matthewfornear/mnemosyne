<?php
// facebook_groups_scraper.php
// PHP port of facebook_groups_scraper.py

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
$COOKIE_FILE = "$PARENT_DIR/settings/cookie.json";
$OUTPUT_FILE = "$PARENT_DIR/output/groups_php.jsonl";
$STATE_FILE = "$PARENT_DIR/output/groups_state_php.json";

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

// --- CONFIGURATION ---
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
    "Connection: keep-alive",
    "Priority: u=1, i",
    "sec-ch-ua: \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Brave\";v=\"138\"",
    "sec-ch-ua-full-version-list: \"Not)A;Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"138.0.0.0\", \"Brave\";v=\"138.0.0.0\"",
    "sec-ch-ua-mobile: ?0",
    "sec-ch-ua-model: \"\"",
    "sec-ch-ua-platform: \"Windows\"",
    "sec-ch-ua-platform-version: \"19.0.0\"",
    "Sec-GPC: 1",
    "x-asbd-id: 359341",
    "x-fb-friendly-name: SearchCometResultsPaginatedResultsQuery",
    "x-fb-lsd: whxXrn___FJH5N9W4OZODD",
];
$DOC_ID = "9960465747398298";
$SEARCH_TEXT = getenv('SEARCH_TEXT') ?: 'Dallas, TX';
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

// --- LOAD COOKIES ---
$COOKIES = load_json($COOKIE_FILE);
if (!$COOKIES || !isset($COOKIES["c_user"])) {
    exit("Cookie file missing or invalid.\n");
}

// --- LOAD STATE ---
if (file_exists($STATE_FILE)) {
    $state = load_json($STATE_FILE);
    $cursor = $state["cursor"] ?? null;
    $seen_ids = array_flip($state["seen_ids"] ?? []);
} else {
    $cursor = null;
    $seen_ids = [];
}

function save_state_php($cursor, $seen_ids, $STATE_FILE) {
    save_json($STATE_FILE, ["cursor" => $cursor, "seen_ids" => array_keys($seen_ids)]);
}

function append_group_php($group, &$seen_ids, $OUTPUT_FILE) {
    if (!isset($seen_ids[$group["id"]])) {
        append_jsonl($OUTPUT_FILE, $group);
        $seen_ids[$group["id"]] = true;
    }
}

// --- USER: Paste your variables JSON from DevTools below ---
$USER_VARIABLES_JSON = '{"allow_streaming":false,"args":{"callsite":"comet:groups_search","config":{"exact_match":false,"high_confidence_config":null,"intercept_config":null,"sts_disambiguation":null,"watch_config":null},"context":{"bsid":"97e85077-b9d2-4f94-9a3b-c1926420c4e5","tsid":null},"experience":{"client_defined_experiences":["ADS_PARALLEL_FETCH"],"encoded_server_defined_params":null,"fbid":null,"type":"GROUPS_TAB_GLOBAL"},"filters":[],"text":"Dallas, TX"},"count":5,"cursor":null,"feedLocation":"SEARCH","feedbackSource":23,"fetch_filters":true,"focusCommentID":null,"locale":null,"privacySelectorRenderLocation":"COMET_STREAM","renderLocation":"search_results_page","scale":1,"stream_initial_count":0,"useDefaultActor":false,"__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider":false,"__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider":false,"__relay_internal__pv__IsWorkUserrelayprovider":false,"__relay_internal__pv__FBReels_deprecate_short_form_video_context_gkrelayprovider":true,"__relay_internal__pv__FeedDeepDiveTopicPillThreadViewEnabledrelayprovider":false,"__relay_internal__pv__CometImmersivePhotoCanUserDisable3DMotionrelayprovider":false,"__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider":false,"__relay_internal__pv__IsMergQAPollsrelayprovider":false,"__relay_internal__pv__FBReelsMediaFooter_comet_enable_reels_ads_gkrelayprovider":true,"__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider":false,"__relay_internal__pv__CometUFIShareActionMigrationrelayprovider":true,"__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider":false,"__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider":true,"__relay_internal__pv__FBReelsIFUTileContent_reelsIFUPlayOnHoverrelayprovider":true}';

function fetch_page_php($cursor, $COOKIES, $HEADERS, $PROXIES, $USER_VARIABLES_JSON, $DOC_ID) {
    $user_id = $COOKIES["c_user"];
    $variables = json_decode($USER_VARIABLES_JSON, true);
    $variables["cursor"] = $cursor;
    $data = [
        "av" => $user_id,
        "__aaid" => "0",
        "__user" => $user_id,
        "__a" => "1",
        "__req" => "9",
        "__hs" => "20285.HYP:comet_pkg.2.1...0",
        "dpr" => "1",
        "__ccg" => "EXCELLENT",
        "__rev" => "1024785322",
        "__s" => "7h5qe5:gm7xfb:f8aoy1",
        "__hsi" => "7527480471665268846",
        "__dyn" => "7xeUjGU5a5Q1ryaxG4Vp41twpUnwgU29zEdE98K360CEboG0IE6u3y4o2Gwfi0LVE4W0qa321Rw8G11wBz81s8hwGxu782lwv89k2C0iK1awhUC7Udo5qfK0zEkxe2GewGwkUe9obrwh8lwuEjxuu3W3y261kx-0iu2-awLyES0gl08O321LwTwKG4UrwFg2fwxyo6J0qo5u1JwjHDzUiBG2OUqwjVqwLwHwea1wweW2K3a2q",
        "__csr" => "gB3c8hQBE8OZEl5gxdbibnv6d9jd5idFikN7lpf9bl9jp5QXGybJBTIxuhkjmGl7eGiLGJ5BmiJmiN7y9W-4poSEyEOUCRiVXBDz8b8l-rxGE-5RAgG8xm5UaUWqeyUry8nwxxi8wCV8qwCxq1TxyEaECuUjwgo4C3W25a1NUpx21kgfUO4Uy4U6idzUgwwwNwRU4e2W15xO48qxqE2rwyw964poy4oeu2KbwKDwWwmU2lw5OG090zUN049c0uibw19S0vG0u2m1eyIy835w3Z8075N5hk00_5E15U1gkb83u0GE2Ow7gw0QDQ051Ekw1wS0mitm0byP011R0q606LUiwf20ky02qm4E0aGo0Cm046Q0qu065o0gsw4SK0kO0uW8m",
        "__hsdp" => "gqMB2isG86VUgh0wky98PaVVHxe5SuCjSO9Cle6jel9288Q44i4QEsG8gIwJa13DDG88LxGhrXkUyEOAfx2lGdAmQHx3yER68-kLqbsiijhbAJAAmazGVzkHQa4Fa8GKQOy9JABjAraZjlkRlFbW58RfaxREp4aNtEaIV2FdAySA4t8wEx2i7Ozkp11aiaCy6eBGmdxuax67SEtyVGz8gBxuS8zGx2iAbByKunBDAVrzl8EG6oyWBQhqPwIGfiGaGaAJi124WoSAi68O44cx2ExAEqnyWymu2cwlxam5UG4UG2C5o9U8olwyyU7-0yF4exSQ12wgQ6UsUGufxIxp5gjDxC1-wAwBwaq2S9BHU188cE5G0si0n60TQqp3aoix-fweK3a78147apmaf9orweO1Hwpoeo2GwcS1xwn80Im04zA3u0nu0k60ME2gx28x61Rz82cwRw3Fo7S0ve08Jw3dE1H80hSw",
        "__hblp" => "053wUw5uwGw2E9U30w4RwnESawh86K0MU7a0oq1Bw7gw2CE1LV80Eq7Q1zw13G0GE3dws84S0kO0ny0V834w8K09oxm0rm0iq0wEfU2Gw9i0z8doiwTw4kwaO0py1Zw7Pw2bo6mfxq0aFg1GU0Au3K0va0Xo880A5028E",
        "__sjsp" => "gplvl8I99GHUrDx259opwGxCuCjD8CpkUpJe9gb42K7WwAwhVVULixWhoNm2S5CEScG589rVpoWewCwwwfqq585V12do5K58mxu0IrocbwgoqwgE5O7o4h0YxS2CeyEy2cwlw4FwvU2awOJ08p0UUGu2IwFk4U8o2zwcO2S9BHU188cE0V60TQqp3aoiwLweK0m1OClyzOm0gy062o0ieg1HU0Fm48qwto",
        "__comet_req" => "15",
        "fb_dtsg" => "NAfsLMAoygtUMjjy79WCqgvelx-UTNq-pgw9OCueXPBPXVa04ebBe9A:43:1750474603",
        "jazoest" => "25567",
        "lsd" => "whxXrn___FJH5N9W4OZODD",
        "__spin_r" => "1024785322",
        "__spin_b" => "trunk",
        "__spin_t" => "1752628123",
        "__crn" => "comet.fbweb.CometGroupsSearchRoute",
        "fb_api_caller_class" => "RelayModern",
        "fb_api_req_friendly_name" => "SearchCometResultsPaginatedResultsQuery",
        "variables" => json_encode($variables),
        "server_timestamps" => "true",
        "doc_id" => $DOC_ID,
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
    // Enable auto-decompression for gzip/deflate/brotli if possible
    curl_setopt($ch, CURLOPT_ENCODING, '');
    // Include headers in the output
    curl_setopt($ch, CURLOPT_HEADER, true);
    $resp = curl_exec($ch);
    if ($resp === false) {
        $error = curl_error($ch);
        curl_close($ch);
        throw new Exception("cURL error: $error");
    }
    $header_size = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
    $headers = substr($resp, 0, $header_size);
    $body = substr($resp, $header_size);
    $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    $json = json_decode($body, true);
    if ($json !== null) return $json;
    // If not JSON, print headers and a snippet of the body for debugging
    $snippet = substr($body, 0, 500);
    echo "\n--- RESPONSE HEADERS ---\n$headers\n--- END HEADERS ---\n";
    echo "\n--- SANITY CHECK: Non-JSON response (first 500 chars) ---\n$snippet\n--- END SANITY CHECK ---\n";
    append_jsonl($GLOBALS['OUTPUT_FILE'], ["id" => "unknown", "html_response" => $body]);
    throw new Exception("Non-JSON response, saved to output.");
}

function extract_groups_php($response) {
    $groups = [];
    try {
        $edges = $response["data"]["serpResponse"]["results"]["edges"];
        foreach ($edges as $edge) {
            $node = $edge["rendering_strategy"]["view_model"]["profile"] ?? [];
            if (($node["__typename"] ?? null) === "Group") {
                $groups[] = [
                    "id" => $node["id"] ?? null,
                    "name" => $node["name"] ?? null,
                    "url" => $node["url"] ?? ($node["profile_url"] ?? null),
                ];
            }
        }
    } catch (Exception $e) {
        echo "Error extracting groups: {$e->getMessage()}\n";
    }
    return $groups;
}

function get_next_cursor_php($response) {
    return $response["data"]["serpResponse"]["results"]["page_info"]["end_cursor"] ?? null;
}

// --- Search Input Selection ---
echo "Choose search input mode: [1] Use facebook_group_urls.txt [2] Enter search manually\n";
$search_mode = prompt("Enter 1 or 2 (default 1): ");
$search_queries = [];
if ($search_mode === "2") {
    $manual_search = prompt("Enter your search text (e.g. Dallas, TX): ");
    $search_queries[] = $manual_search;
} else {
    $url_file = "$PARENT_DIR/settings/facebook_group_urls.txt";
    if (!file_exists($url_file)) {
        exit("facebook_group_urls.txt not found in settings directory.\n");
    }
    $lines = file($url_file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($lines as $line) {
        // Extract the query from the URL
        if (preg_match('/[?&]q=([^&]+)/', $line, $matches)) {
            $decoded = urldecode($matches[1]);
            $search_queries[] = $decoded;
            $no_comma = str_replace(',', '', $decoded);
            if ($no_comma !== $decoded) {
                $search_queries[] = $no_comma;
            }
        }
    }
    // Remove duplicates
    $search_queries = array_unique($search_queries);
    if (empty($search_queries)) {
        exit("No valid search queries found in facebook_group_urls.txt.\n");
    }
}

// --- MAIN LOOP ---
foreach ($search_queries as $i => $SEARCH_TEXT) {
    $total_queries = count($search_queries);
    echo "\n==== Running search ".($i+1)."/$total_queries: $SEARCH_TEXT ====" . "\n";
    // Reset cursor and seen_ids for each search
    if (file_exists($STATE_FILE)) {
        $state = load_json($STATE_FILE);
        $cursor = $state["cursor"] ?? null;
        $seen_ids = array_flip($state["seen_ids"] ?? []);
    } else {
        $cursor = null;
        $seen_ids = [];
    }
    $done = false;
    while (!$done) {
        echo "Fetching page with cursor: ".($cursor ?: 'null')."\n";
        try {
            // Update USER_VARIABLES_JSON with the current search text
            $variables = json_decode($USER_VARIABLES_JSON, true);
            $variables["text"] = $SEARCH_TEXT;
            if (isset($variables["args"])) {
                $variables["args"]["text"] = $SEARCH_TEXT;
            }
            $USER_VARIABLES_JSON_THIS = json_encode($variables);
            // Print POST data for debugging (only for this query)
            echo "POST variables: ".$USER_VARIABLES_JSON_THIS."\n";
            $response = fetch_page_php($cursor, $COOKIES, $HEADERS, $PROXIES, $USER_VARIABLES_JSON_THIS, $DOC_ID);
        } catch (Exception $e) {
            echo "Request failed: {$e->getMessage()}\n";
            break;
        }
        $new_groups = 0;
        foreach (extract_groups_php($response) as $group) {
            if (!isset($seen_ids[$group["id"]])) {
                append_group_php($group, $seen_ids, $OUTPUT_FILE);
                $new_groups++;
            }
        }
        save_state_php($cursor, $seen_ids, $STATE_FILE);
        echo "Added $new_groups new groups. Total: ".count($seen_ids)."\n";
        $next_cursor = get_next_cursor_php($response);
        if (!$next_cursor || $new_groups == 0) {
            echo "No more pages or no new groups found.\n";
            break;
        }
        $cursor = $next_cursor;
        random_sleep($SLEEP_BETWEEN_REQUESTS[0], $SLEEP_BETWEEN_REQUESTS[1]);
    }
} 