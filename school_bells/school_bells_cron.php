<?php

//restrict to command line only
if (defined('STDIN')) {
	$document_root = dirname(__DIR__, 2);
	set_include_path($document_root);
	require_once "resources/require.php";
	require_once "resources/classes/text.php";
	require_once "resources/pdo.php";
	require_once __DIR__ . "/school_bell_security.php";
	$_SERVER["DOCUMENT_ROOT"] = $document_root;
}
else {
	die('access denied');
}

$debug = false;
set_time_limit(55);
$current_timestamp = time();

function school_bell_cron_true($value) {
	return $value === true || $value === 1 || $value === '1' || $value === 't' || $value === 'true';
}

$sql = "SELECT ";
$sql .= "v_domains.domain_name AS context, ";
$sql .= "b.domain_uuid, ";
$sql .= "b.school_bell_uuid, ";
$sql .= "b.school_bell_name, ";
$sql .= "b.school_bell_leg_a_data AS extension, ";
$sql .= "b.school_bell_leg_b_type AS full_path, ";
$sql .= "b.school_bell_ring_timeout AS ring_timeout, ";
$sql .= "b.school_bell_timezone AS timezone, ";
$sql .= "b.school_bell_monday, ";
$sql .= "b.school_bell_tuesday, ";
$sql .= "b.school_bell_wednesday, ";
$sql .= "b.school_bell_thursday, ";
$sql .= "b.school_bell_friday, ";
$sql .= "b.school_bell_saturday, ";
$sql .= "b.school_bell_sunday, ";
$sql .= "t.school_bell_hour AS hour, ";
$sql .= "t.school_bell_minute AS minute ";
$sql .= "FROM v_school_bells b ";
$sql .= "JOIN v_domains ON v_domains.domain_uuid = b.domain_uuid ";
$sql .= "JOIN v_school_bell_times t ";
$sql .= "ON t.school_bell_uuid = b.school_bell_uuid ";
$sql .= "AND t.domain_uuid = b.domain_uuid ";
$sql .= "WHERE b.school_bell_enabled = 'true' ";
$sql .= "ORDER BY b.school_bell_name, t.school_bell_order, t.school_bell_hour, t.school_bell_minute";

$database = new database;

// Prevent duplicate scheduler runs on the same host and across PostgreSQL clients.
$school_bell_lock_handle = fopen(sys_get_temp_dir().'/fusionpbx-school-bells.lock', 'c');
if (!$school_bell_lock_handle || !flock($school_bell_lock_handle, LOCK_EX | LOCK_NB)) {
	if (is_resource($school_bell_lock_handle)) fclose($school_bell_lock_handle);
	if ($debug) echo "scheduler already running\n";
	return;
}
register_shutdown_function(function () use ($school_bell_lock_handle) {
	if (is_resource($school_bell_lock_handle)) {
		flock($school_bell_lock_handle, LOCK_UN);
		fclose($school_bell_lock_handle);
	}
});

$school_bell_advisory_lock = false;
if (isset($db) && $db instanceof PDO && $db->getAttribute(PDO::ATTR_DRIVER_NAME) === 'pgsql') {
	$lock_stmt = $db->query("SELECT pg_try_advisory_lock(hashtext('fusionpbx-school-bells-scheduler'))");
	$school_bell_advisory_lock = (bool)$lock_stmt->fetchColumn();
	if (!$school_bell_advisory_lock) {
		if ($debug) echo "scheduler database lock is held\n";
		return;
	}
	register_shutdown_function(function () use ($db) {
		$db->query("SELECT pg_advisory_unlock(hashtext('fusionpbx-school-bells-scheduler'))");
	});
}

$school_bells = $database->select($sql, null, 'all');

if (!is_array($school_bells) || count($school_bells) === 0) {
	if ($debug) {
		echo "no enabled bell times\n";
	}
	return;
}

$event_socket_ip = $_SESSION['event_socket_ip_address'] ?? null;
$event_socket_port = $_SESSION['event_socket_port'] ?? null;
$event_socket_password = $_SESSION['event_socket_password'] ?? null;
if ((!$event_socket_ip || !$event_socket_port || $event_socket_password === null) && isset($db) && $db instanceof PDO) {
    try {
        $settings_stmt = $db->query("SELECT event_socket_ip_address,event_socket_port,event_socket_password FROM v_settings LIMIT 1");
        $settings = $settings_stmt ? $settings_stmt->fetch(PDO::FETCH_ASSOC) : false;
        if (is_array($settings)) {
            $event_socket_ip = $event_socket_ip ?: $settings['event_socket_ip_address'];
            $event_socket_port = $event_socket_port ?: $settings['event_socket_port'];
            if ($event_socket_password === null) $event_socket_password = $settings['event_socket_password'];
        }
    } catch (Throwable $e) {
        error_log('School Bells could not load Event Socket settings: '.$e->getMessage());
    }
}

$freeswitch_event_socket = event_socket_create(
	$event_socket_ip,
	$event_socket_port,
	$event_socket_password
);

if (!$freeswitch_event_socket) {
	die("Cannot connect to Event socket");
}

// A pause applies to the schedule's own local calendar date.
// Read once per invocation, then check before each originate.
$pause_sql = "SELECT domain_uuid, school_bell_uuid, pause_date FROM v_school_bell_pauses";
$pause_rows = $database->select($pause_sql, null, 'all');
$pause_lookup = [];
if (is_array($pause_rows)) {
    foreach ($pause_rows as $p) {
        $pause_lookup[$p['domain_uuid']][$p['school_bell_uuid']][$p['pause_date']] = true;
    }
}

// Calendar exceptions are evaluated against each schedule's local date.
$exception_sql = "SELECT e.domain_uuid, e.start_date, e.end_date, e.recurrence_type, e.all_schedules, m.school_bell_uuid FROM v_school_bell_exceptions e LEFT JOIN v_school_bell_exception_schedules m ON m.school_bell_exception_uuid = e.school_bell_exception_uuid WHERE e.enabled = true";
$exception_rows = $database->select($exception_sql, null, 'all');
$exception_lookup = [];
if (is_array($exception_rows)) {
    foreach ($exception_rows as $e) $exception_lookup[$e['domain_uuid']][] = $e;
}
function school_bell_exception_matches($exception, $schedule_uuid, $local_date) {
    if (!school_bell_cron_true($exception['all_schedules']) && $exception['school_bell_uuid'] !== $schedule_uuid) return false;
    $start = $exception['start_date']; $end = $exception['end_date'];
    if ($exception['recurrence_type'] === 'annual') {
        // Annual recurrence supports ranges spanning Dec 31, including leap-day dates.
        $md = substr($local_date, 5); $first = substr($start, 5); $last = substr($end, 5);
        return $first <= $last ? ($md >= $first && $md <= $last) : ($md >= $first || $md <= $last);
    }
    return $local_date >= $start && $local_date <= $end;
}

// Logging is best-effort. A logging outage must never prevent a scheduled bell.
// History records FreeSWITCH command submission, not confirmation that speakers played audio.
try {
    require_once "resources/pdo.php";
    $sb3_history_stmt = (isset($db) && $db instanceof PDO)
        ? $db->prepare("INSERT INTO v_school_bell_history (school_bell_history_uuid,domain_uuid,school_bell_uuid,schedule_name,scheduled_local_date,scheduled_local_time,schedule_timezone,decision,detail,created_at) VALUES (:uuid,:domain,:schedule,:name,:local_date,:local_time,:timezone,:decision,:detail,:created_at)")
        : null;
} catch (Throwable $e) {
    error_log('School Bells history initialization failed: '.$e->getMessage());
    $sb3_history_stmt = null;
}
function sb3_record_history($stmt,$bell,$local,$decision,$detail='') {
    if (!$stmt) return;
    try {
        $uuid=sprintf('%s-%s-%s-%s-%s',bin2hex(random_bytes(4)),bin2hex(random_bytes(2)),'4'.substr(bin2hex(random_bytes(2)),1),dechex((hexdec(substr(bin2hex(random_bytes(2)),0,1)) & 3) | 8).substr(bin2hex(random_bytes(2)),1),bin2hex(random_bytes(6)));
        $stmt->execute([
            ':uuid'=>$uuid, ':domain'=>$bell['domain_uuid'], ':schedule'=>$bell['school_bell_uuid'],
            ':name'=>$bell['school_bell_name'], ':local_date'=>$local->format('Y-m-d'),
            ':local_time'=>$local->format('H:i:s'), ':timezone'=>$local->getTimezone()->getName(),
            ':decision'=>$decision, ':detail'=>$detail,
            ':created_at'=>gmdate('Y-m-d H:i:s')
        ]);
    } catch (Throwable $e) { error_log('School Bells history insert failed: '.$e->getMessage()); }
}

function sb3_slot_uuid($bell,$local) {
    $slot = $local->format('Y-m-d').' '.sprintf('%02d:%02d', (int)$bell['hour'], (int)$bell['minute']);
    $hex = substr(hash('sha256', $bell['domain_uuid'].'|'.$bell['school_bell_uuid'].'|'.$slot), 0, 32);
    return substr($hex,0,8).'-'.substr($hex,8,4).'-4'.substr($hex,13,3).'-8'.substr($hex,17,3).'-'.substr($hex,20,12);
}
function sb3_claim_slot($db,$bell,$local) {
    if (!$db instanceof PDO) return true;
    $uuid = sb3_slot_uuid($bell,$local);
    try {
        $claim = $db->prepare("INSERT INTO v_school_bell_history (school_bell_history_uuid,domain_uuid,school_bell_uuid,schedule_name,scheduled_local_date,scheduled_local_time,schedule_timezone,decision,detail,created_at) VALUES (:uuid,:domain,:schedule,:name,:local_date,:local_time,:timezone,'claimed','Scheduler slot claimed',:created_at)");
        $claim->execute([
            ':uuid'=>$uuid, ':domain'=>$bell['domain_uuid'], ':schedule'=>$bell['school_bell_uuid'],
            ':name'=>$bell['school_bell_name'], ':local_date'=>$local->format('Y-m-d'),
            ':local_time'=>$local->format('H:i:s'), ':timezone'=>$local->getTimezone()->getName(),
            ':created_at'=>gmdate('Y-m-d H:i:s')
        ]);
        return true;
    } catch (Throwable $e) {
        // The deterministic primary key makes this an atomic cross-process claim.
        return false;
    }
}
function sb3_finish_slot($db,$bell,$local,$decision,$detail='') {
    if (!$db instanceof PDO) return;
    $uuid = sb3_slot_uuid($bell,$local);
    try {
        $update = $db->prepare('UPDATE v_school_bell_history SET decision=:decision, detail=:detail WHERE school_bell_history_uuid=:uuid');
        $update->execute([':decision'=>$decision, ':detail'=>$detail, ':uuid'=>$uuid]);
    } catch (Throwable $e) { error_log('School Bells history update failed: '.$e->getMessage()); }
}

$day_fields = [
	0 => 'school_bell_sunday',
	1 => 'school_bell_monday',
	2 => 'school_bell_tuesday',
	3 => 'school_bell_wednesday',
	4 => 'school_bell_thursday',
	5 => 'school_bell_friday',
	6 => 'school_bell_saturday'
];

foreach ($school_bells as $school_bell) {

	$timezone = trim((string)$school_bell['timezone']);

	if ($timezone === '' || !in_array($timezone, timezone_identifiers_list(), true)) {
		$timezone = date_default_timezone_get();
	}

	$local = new DateTime('@'.$current_timestamp);
	$local->setTimezone(new DateTimeZone($timezone));

	$current_dow = (int)$local->format('w');
	$current_hour = (int)$local->format('G');
	$current_minute = (int)$local->format('i');

	$day_field = $day_fields[$current_dow];

	if (!school_bell_cron_true($school_bell[$day_field])) {
		continue;
	}

	if ($current_hour !== (int)$school_bell['hour']) {
		continue;
	}

	if ($current_minute !== (int)$school_bell['minute']) {
		continue;
	}

    $local_date = $local->format('Y-m-d');
    if (isset($pause_lookup[$school_bell['domain_uuid']][$school_bell['school_bell_uuid']][$local_date])) {
        sb3_record_history($sb3_history_stmt,$school_bell,$local,'skipped_pause','Skip Today');
        if ($debug) echo "Skipped paused schedule: ".$school_bell['school_bell_name']." (".$local_date.")\n";
        continue;
    }

    $excepted = false;
    foreach ($exception_lookup[$school_bell['domain_uuid']] ?? [] as $exception) {
        if (school_bell_exception_matches($exception, $school_bell['school_bell_uuid'], $local_date)) {
            $excepted = true;
            break;
        }
    }
    if ($excepted) {
        sb3_record_history($sb3_history_stmt,$school_bell,$local,'skipped_exception','Calendar exception');
        if ($debug) echo "Skipped calendar exception: ".$school_bell['school_bell_name']." (".$local_date.")\n";
        continue;
    }

	$ring_timeout = (int)$school_bell['ring_timeout'];

	if ($ring_timeout <= 0) {
		$ring_timeout = 5;
	}

	if ($ring_timeout > 60) {
		$ring_timeout = 60;
	}

	if (!school_bell_valid_destination($school_bell['extension'])
        || !school_bell_valid_context($school_bell['context'])
        || !school_bell_valid_recording_path($school_bell['full_path'])) {
        sb3_record_history($sb3_history_stmt,$school_bell,$local,'request_error','Bell destination or recording is invalid');
        error_log('School Bells rejected invalid command data for schedule '.$school_bell['school_bell_uuid']);
        continue;
    }

    if (!sb3_claim_slot($db, $school_bell, $local)) {
        if ($debug) echo "Already claimed: ".$school_bell['school_bell_name']." (".$local_date." ".$local->format('H:i').")\n";
        continue;
    }

	$switch_cmd = "bgapi ";
	$switch_cmd .= "originate {ignore_early_media=true,";
	$switch_cmd .= "hangup_after_bridge=true,";
	$switch_cmd .= "domain_name=".$school_bell['context'].",";
	$switch_cmd .= "domain_uuid=".$school_bell['domain_uuid'].",";
	$switch_cmd .= "call_timeout=".$ring_timeout."}";
	$switch_cmd .= "loopback/".$school_bell['extension']."/".$school_bell['context'];
	$switch_cmd .= " &playback(".$school_bell['full_path'].")";

	if ($debug) {
		echo "[".$local->format('Y-m-d H:i:s T')."] ".$school_bell['school_bell_name']."\n";
		echo $switch_cmd."\n";
	}

    try {
        $reply=event_socket_request($freeswitch_event_socket, $switch_cmd);
        $failed=($reply === false || (is_string($reply) && strpos($reply,'-ERR') !== false));
        sb3_finish_slot($db,$school_bell,$local,$failed?'request_error':'submitted',$failed?'Event socket rejected or failed':'FreeSWITCH request submitted; playback not verified');
    } catch (Throwable $e) {
        sb3_finish_slot($db,$school_bell,$local,'request_error','Event socket request threw an error');
        error_log('School Bells originate failed: '.$e->getMessage());
    }
}

?>
