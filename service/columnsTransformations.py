empNewColumnsNames: dict[str, str] = {
    "idemployee": "id_employee",
    "firstName": "first_name",
    "middleName": "middle_name",
    "lastName1": "last_name1",
    "lastName2": "last_name2",
    "fullName": "full_name",
    "fullNameWithUserName": "full_name_with_user_name",
    "firstNameLastNameWithUserName": "first_name_last_name_with_user_name",
    "mailAddress": "mail_address",
    "mailCity": "mail_city",
    "mailState": "mail_state",
    "mailZipCode": "mail_zip_code",
    "physicalAddress": "physical_address",
    "physicalCity": "physical_city",
    "physicalState": "physical_state",
    "physicalZipCode": "physical_zip_code",
    "pyshicalCounty": "pyshical_county",
    "phone": "phone",
    "cellPhone": "cell_phone",
    "emailPersonal": "email_personal",
    "emailWork": "email_work",
    "username": "username",
    "birthDay": "birthday",
    "image": "image",
    "idGender": "id_gender",
    "idMarital": "id_marital",
    "dateCreated": "date_created",
    "lastUpdated": "last_updated",
    "auth0UserId": "auth0_user_id",
    "active": "active"
}

rpNewColumnNames: dict[str, str] = {
    "statusReceipt": "status_receipt",
    "txnType": "txn_type",
    "idReceiptHdr": "id_receipt_hdr",
    "date": "date",
    "balanceDue": "balance_due",
    "idCustomer": "customer_id",
    "firstName": "first_name",
    "lastName1": "last_name1",
    "customerName": "customer_name",
    "invoiceItemDesc": "invoice_item_desc",
    "payee": "payee",
    "fiduciary": "fiduciary",
    "nonFiduciary": "non_fiduciary",
    "amountII": "amount_ii",
    "amountPaidRec": "amount_paid_rec",
    "payMethods": "pay_methods",
    "totalAmntReceipt": "total_amnt_receipt",
    "memo": "memo",
    "usr": "usr",
    "csr": "csr",
    "csR2": "cs_r2",
    "agency": "agency",
    "officeRec": "office_rec",
    "officePol": "office_pol",
    "bankAccount": "bank_account",
    "retained": "retained",
    "void": "void",
    "for_t": "for",
    "just_date": "just_date",
    "just_time": "just_time"
}

rpColumnsToDelete: list[str] = [
    "status_receipt",
    "txn_type",
    "date",
    "balance_due",
    "first_name",
    "last_name1",
    "invoice_item_desc",
    "payee",
    "fiduciary",
    "non_fiduciary",
    "amount_ii",
    "amount_paid_rec",
    "pay_methods",
    "total_amnt_receipt",
    "memo",
    "cs_r2",
    "agency",
    "office_pol",
    "bank_account",
    "retained",
    "void"
]

rpValuesToFilter: list[str] = [
    "NewB - EFT To Company",
    "BF",
    "BF 12 Paid In Full",
    "BF 6 Paid In Full",
    "BF Endo Fee",
    "Discount Card",
    "Payment Fee",
    "Late Payment Fee",
    "Payment Fee Commercial",
    "Invoice",
    "Late Fee Invoice",
    "BF DMV Reg",
    "BF Title Change",
    "BF DMV reg- DUPLICATE",
    "BF Release Liability",
    "BF Replacement Plates",
    "BF TRIP REQ",
    "BF Adrianas Towing",
    "BF Permit",
    "BF Traffic School",    
    "BF Renewal Only",
    "BF Trucking",
    "Immigration Services"
]

custColumnsToDelete: list[str] = [
    "business_name",
    "first_name",
    "middle_name",
    "last_name1",
    "last_name2",
    "full_name",
    "id_marital_status",
    "id_gender",
    "mail_address",
    "mail_city",
    "mail_state",
    "mail_zip_code",
    "mail_county",
    "physical_address",
    "physical_county",
    "website",
    "birthday",
    "dob",
    "image",
    "license_number",
    "id_cust_type",
    "cust_type_name",
    "date_created",
    "last_updated",
    "occupation",
    "active"
]

custNewColumnsNames: dict[str, str] = {
    "idCustomer": "customer_id",
    "businessName": "business_name",
    "firstName": "first_name",
    "middleName": "middle_name",
    "lastName1": "last_name1",
    "lastName2": "last_name2",
    "fullName": "full_name",
    "idMaritalStatus": "id_marital_status",
    "maritalName": "marital_name",
    "idGender": "id_gender",
    "genderName": "gender_name",
    "mailAddress": "mail_address",
    "mailCity": "mail_city",
    "mailState": "mail_state",
    "mailZipCode": "mail_zip_code",
    "mailCounty": "mail_county",
    "physicalAddress": "physical_address",
    "physicalCity": "physical_city",
    "physicalState": "physical_state",
    "physicalZipCode": "physical_zip_code",
    "physicalCounty": "physical_county",
    "phone": "phone",
    "cellPhone": "cell_phone",
    "email": "email",
    "website": "website",
    "birthDay": "birthday",
    "dob": "dob",
    "age": "age",
    "image": "image",
    "licenseNumber": "license_number",
    "idCustType": "id_cust_type",
    "custTypeName": "cust_type_name",
    "dateCreated": "date_created",
    "lastUpdated": "last_updated",
    "occupation": "occupation",
    "active": "active"
}

laeDataColumnsNewOrder: list[str] = [
    "just_date",
    "just_time",
    "customer_id",
    "customer_name",
    "gender_name",
    "marital_name",
    "age",
    "physical_zip_code",
    "physical_state",
    "physical_city",
    "email",
    "cell_phone",
    "phone",
    "phone_fix",
    "for",
    "usr",
    "csr",
    "office_rec",
    "nb",
    "bf",
    "endos",
    "payments",
    "invoice",
    "dmv",
    "towing",
    "permit",
    "traffic_school",
    "renewal",
    "trucking",
    "immigration"
]

polNewColumnNames: dict[str, str] = {
    "idPoliciesHdr": "id_policies_hdr",
    "policyNumber": "policy_number",
    "accountNumber": "account_number",
    "idStatus": "id_status",
    "status": "status",
    "idCustomer": "id_customer",
    "customername": "customername",
    "idCustType": "id_cust_type",
    "custTypeName": "cust_type_name",
    "idEmployeeUSR": "id_employee_usr",
    "nameEmployeeUSR": "name_employee_usr",
    "userNameUSR": "user_name_usr",
    "idEmployeeCSR1": "id_employee_csr1",
    "nameEmployeeCSR1": "name_employee_csr1",
    "userNameCSR1": "user_name_csr1",
    "idEmployeeCSR2": "id_employee_csr2",
    "nameEmployeeCSR2": "name_employee_csr2",
    "userNameCSR2": "user_name_csr2",
    "idEmployeeUW": "id_employee_uw",
    "nameEmployeeUW": "name_employee_uw",
    "idProgram": "id_program",
    "programName": "program_name",
    "idCiaCovLimit": "id_cia_cov_limit",
    "idLimit": "id_limit",
    "limitValue": "limit_value",
    "coverageName": "coverage_name",
    "idCompany": "id_company",
    "insuredCompany": "insured_company",
    "insuredCompanyShortName": "insured_company_short_name",
    "insuredCompanyPhone": "insured_company_phone",
    "insuredCompanyAddr": "insured_company_addr",
    "insuredCompanyWebsite": "insured_company_website",
    "idDepartment": "id_department",
    "departmentName": "department_name",
    "effectiveDate": "effective_date",
    "expirationDate": "expiration_date",
    "amountPolicy": "amount_policy",
    "totalBalances": "total_balances",
    "idBuildingInsured": "id_building_insured",
    "idOffice": "id_office",
    "office": "office",
    "idCompanyAGI": "id_company_agi",
    "agiCompanyName": "agi_company_name",
    "idSourceContact": "id_source_contact",
    "sourceName": "source_name",
    "idPolicyTerm": "id_policy_term",
    "termName": "term_name",
    "idPolBillType": "id_pol_bill_type",
    "billingTypeName": "billing_type_name",
    "idPolPayPlan": "id_pol_pay_plan",
    "paymentPlanName": "payment_plan_name",
    "recordIdQuoteTr": "record_id_quote_tr",
    "quoteNumberTr": "quote_number_tr",
    "quoteUrltr": "quote_url_tr",
    "quoteDate": "quote_date",
    "revisionDate": "revision_date",
    "workDate": "work_date",
    "pathFiles": "path_files",
    "pathFilesS3": "path_files_s3",
    "isSuspense": "is_suspense",
    "isReviewed": "is_reviewed",
    "nonOwner": "non_owner",
    "producerCode": "producer_code",
    "bfAmount": "bf_amount",
    "dateCreated": "date_created",
    "lastUpdated": "last_updated",
    "active": "active",
    "policiesDTL": "policies_dtl",
    "receipts": "receipts",
    "benefitsCustomers": "benefits_customers",
    "vehicleInsureds": "vehicle_insureds",
    "claims": "claims",
    "uwQuestions": "uw_questions",
    "policyErrorTags": "policy_error_tags",
    "logs": "logs",
    "tabs": "tabs",
    "locations": "locations"
}

polColumnsToDelete: list[str] = [
    "policies_dtl",
    "receipts",
    "benefits_customers",
    "claims",
    "uw_questions",
    "policy_error_tags",
    "logs",
    "tabs",
    "locations"
]

vehNewColumnNames: dict[str, str] = {
    "idPolicieHdr": "id_policie_hdr",
    "idvehicleInsured": "id_vehicle_insured",
    "idProduct": "id_product",
    "producTname": "product_name",
    "vehicleBodyType": "vehicle_body_type",
    "idVehicleType": "id_vehicle_type",
    "vehicleTypeName": "vehicle_type_name",
    "idVehicleUse": "id_vehicle_use",
    "vehicleUseName": "vehicle_use_name",
    "vinnumber": "vin_number",
    "fleetId": "fleet_id",
    "year": "year",
    "make": "make",
    "model": "model",
    "gvw": "gvw",
    "sic": "sic",
    "sym": "sym",
    "compSym": "comp_sym",
    "collSym": "coll_sym",
    "commuteMilles": "commute_milles",
    "annualMilles": "annual_milles",
    "odometer": "odometer",
    "odometerDate": "odometer_date",
    "radius": "radius",
    "farthestTerminal": "farthest_terminal",
    "territory": "territory",
    "zipCode": "zip_code",
    "factor": "factor",
    "driverClass": "driver_class",
    "licensePlate": "license_plate",
    "licenseState": "license_state",
    "driverNo": "driver_no",
    "hotCar": "hot_car",
    "companyVehicleNo": "company_vehicle_no",
    "source": "source",
    "passiveSeatBelts": "passive_seat_belts",
    "antiLockBrakes": "anti_lock_brakes",
    "antiLockBrakeType": "anti_lock_brake_type",
    "airBag": "air_bag",
    "airBagType": "air_bag_type",
    "antiTheft": "anti_theft",
    "antiTheftLevelType": "anti_theft_level_type",
    "carPhone": "car_phone",
    "color": "color",
    "coMedPayDed": "co_med_pay_ded",
    "coMedPayLimit": "co_med_pay_limit",
    "convertible": "convertible",
    "coTowingLimit": "co_towing_limit",
    "coUimbilimits1": "co_uim_bi_limits1",
    "coUimbilimits2": "co_uim_bi_limits2",
    "coUimpdlimit": "co_uim_pd_limit",
    "coUmconversion": "co_um_conversion",
    "coUninsBilimits1": "co_unins_bi_limits1",
    "coUninsBilimits2": "co_unins_bi_limits2",
    "coUninsPdded": "co_unins_pd_ded",
    "coUninsPdlimit": "co_unins_pd_limit",
    "funeral": "funeral",
    "funeralLimit": "funeral_limit",
    "funeralPremium": "funeral_premium",
    "liab": "liab",
    "liabBi": "liab_bi",
    "liabBipremium": "liab_bi_premium",
    "liabLimits1": "liab_limits1",
    "liabLimits2": "liab_limits2",
    "liabLimits3": "liab_limits3",
    "liabPd": "liab_pd",
    "medExpense": "med_expense",
    "medExpenseDesc": "med_expense_desc",
    "medicare": "medicare",
    "medPay": "med_pay",
    "medPayDed": "med_pay_ded",
    "medPayLimit": "med_pay_limit",
    "medPayPremium": "med_pay_premium",
    "mexicoCoverage": "mexico_coverage",
    "mexicoPremium": "mexico_premium",
    "militaryPip": "military_pip",
    "msrp": "msrp",
    "numOfCyl": "num_of_cyl",
    "numOfDoors": "num_of_doors",
    "rental": "rental",
    "rentalLimit": "rental_limit",
    "rentalPremium": "rental_premium",
    "rideShare": "ride_share",
    "runningLights": "running_lights",
    "towing": "towing",
    "towingLimit": "towing_limit",
    "towingPremium": "towing_premium",
    "truckSize": "truck_size",
    "truckSizeDesc": "truck_size_desc",
    "ttops": "ttops",
    "turbo": "turbo",
    "twoSeater": "two_seater",
    "uimbi": "uimbi",
    "uimbilimits1": "uim_bi_limits1",
    "uimbilimits2": "uim_bi_limits2",
    "uimbipremium": "uim_bi_premium",
    "uimpd": "uimpd",
    "uimpdlimit": "uimpd_limit",
    "uimpdpremium": "uimpd_premium",
    "umconversion": "um_conversion",
    "uninsBi": "unins_bi",
    "uninsBilimits1": "unins_bi_limits1",
    "uninsBilimit2": "unins_bi_limit2",
    "uninsBipremium": "unins_bi_premium",
    "uninsPd": "unins_pd",
    "uninsPdded": "unins_pd_ded",
    "uninsPdlimit": "unins_pd_limit",
    "uninsPdpremium": "unins_pd_premium",
    "vinetching": "vin_etching",
    "waivedPip": "waived_pip",
    "windowId": "window_id",
    "workLoss": "work_loss",
    "dateCreated": "date_created",
    "lastUpdated": "last_updated",
    "active": "active"
}
