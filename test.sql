CREATE TABLE new_face_recognition_event_data (
    picname VARCHAR(255),
    channel VARCHAR(255),
    alarmtime TIMESTAMP,
    alarmid INT,
    csspicsize INT,
    alarmmsg VARCHAR(255),
    devname VARCHAR(255),
    alarmevent VARCHAR(255),
    serialnumber VARCHAR(255),
    alarmvendor VARCHAR(255),
    picsize FLOAT,
    "group" VARCHAR(50),
    status VARCHAR(50),
    level VARCHAR(50),
    aispush BOOLEAN,
    si_id VARCHAR(255),
    user_id VARCHAR(255),
    source_id VARCHAR(255),
    object_id VARCHAR(255),
    bbox VARCHAR(255),  -- Assuming bbox is stored as a text representation of an array
    lm VARCHAR(255),    -- Assuming lm is stored as a text representation of an array
    confidence FLOAT,
    image_path VARCHAR(255),
    silent_face VARCHAR(50),
    person_id VARCHAR(255),  -- Assuming person_id is stored as a text representation of an array
    message_base64_service_key VARCHAR(255),  -- Adjusted for SQL naming conventions
    message_base64_cam_info_cam_id VARCHAR(255),  -- Adjusted for SQL naming conventions
    message_base64_cam_info_location VARCHAR(255),  -- Adjusted for SQL naming conventions
    message_base64_ai_config_attribute VARCHAR(255)-- Assuming attribute is stored as a text representation of an array
);