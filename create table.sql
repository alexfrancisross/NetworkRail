--DROP TABLE NationalRail;

CREATE TABLE NationalRail
(
 event_type character varying(255),
 gbtt_timestamp timestamp,
 original_loc_stanox character varying(255),
 planned_timestamp timestamp,
 timetable_variation integer,
 original_loc_timestamp timestamp,
 current_train_id character varying(255),
 delay_monitoring_point boolean,
 next_report_run_time character varying(255),
 reporting_stanox character varying(255),
 actual_timestamp timestamp,
 correction_ind boolean,
 event_source character varying(255),
 train_file_address character varying(255),
 platform character varying(255),
 division_code integer,
 train_terminated boolean,
 train_id character varying(255),
 offroute_ind boolean,
 variation_status character varying(255),
 train_service_code integer,
 toc_id integer,
 loc_stanox character varying(255),
 auto_expected boolean,
 direction_ind character varying(255),
 route character varying(255),
 planned_event_type character varying(255),
 next_report_stanox character varying(255),
 line_ind character varying(255)
)





