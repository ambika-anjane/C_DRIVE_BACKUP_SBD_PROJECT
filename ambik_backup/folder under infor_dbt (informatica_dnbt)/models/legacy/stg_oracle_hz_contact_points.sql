with source as (

    select * from {{ source('oracle_master_data', 'hz_contact_points') }}

),

renamed as (

    select
        contact_point_id,
        contact_point_type,
        status,
        owner_table_name,
        owner_table_id,
        primary_flag,
        orig_system_reference,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        wh_update_date,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute16,
        attribute17,
        attribute18,
        attribute19,
        attribute20,
        global_attribute_category,
        global_attribute1,
        global_attribute2,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute10,
        global_attribute11,
        global_attribute12,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute16,
        global_attribute17,
        global_attribute18,
        global_attribute19,
        global_attribute20,
        edi_transaction_handling,
        edi_id_number,
        edi_payment_method,
        edi_payment_format,
        edi_remittance_method,
        edi_remittance_instruction,
        edi_tp_header_id,
        edi_ece_tp_location_code,
        email_format,
        email_address,
        best_time_to_contact_start,
        best_time_to_contact_end,
        phone_calling_calendar,
        contact_attempts,
        contacts,
        declared_business_phone_flag,
        do_not_use_flag,
        do_not_use_reason,
        last_contact_dt_time,
        phone_preferred_order,
        priority_of_use_code,
        telephone_type,
        time_zone,
        phone_touch_tone_type_flag,
        phone_area_code,
        phone_country_code,
        phone_number,
        phone_extension,
        phone_line_type,
        telex_number,
        web_type,
        url,
        content_source_type,
        raw_phone_number,
        object_version_number,
        created_by_module,
        application_id,
        timezone_id,
        contact_point_purpose,
        primary_by_purpose,
        transposed_phone_number,
        eft_transmission_program_id,
        eft_printing_program_id,
        eft_user_number,
        eft_swift_code,
        actual_content_source,
        _batch_run_id,
        _batch_insert_date,
        _batch_update_date,
        _source_id

    from source

)

select * from renamed