with source as (

    select * from {{ source('oracle_master_data', 'hz_parties') }}

),

renamed as (

    select
       party_id,
       party_number,
       party_name,
       party_type,
       validated_flag,
       last_updated_by,
       creation_date,
       last_update_login,
       request_id,
       program_application_id,
       created_by,
       last_update_date,
       program_id,
       program_update_date,
       wh_update_date,
       attribute_category,
       attribute1,
       attribute2,
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
       attribute21,
       attribute22,
       attribute23,
       attribute24,
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
       orig_system_reference,
       sic_code,
       hq_branch_ind,
       customer_key,
       tax_reference,
       jgzz_fiscal_code,
       duns_number,
       tax_name,
       person_pre_name_adjunct,
       person_first_name,
       person_middle_name,
       person_last_name,
       person_name_suffix,
       person_title,
       person_academic_title,
       person_previous_last_name,
       known_as,
       person_iden_type,
       person_identifier,
       group_type,
       country,
       



    from source

)

select * from renamed
