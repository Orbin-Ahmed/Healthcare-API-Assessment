import requests
import json
import time
import os
from dotenv import load_dotenv

# config 
load_dotenv()

BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")
headers = {
    "x-api-key": API_KEY
}

# BP parser function 
def parse_blood_pressure(bp_value):
    if bp_value is None:
        print("    Missing BP field")
        return None, None, True
    
    # Convert to string
    bp_str = str(bp_value).strip()
    
    # Check for invalid strings
    if bp_str in ['N/A', 'n/a', 'INVALID_BP_FORMAT', '']:
        print(f"Invalid BP string: {bp_str}")
        return None, None, True
    
    if '/' in bp_str:
        parts = bp_str.split('/')

        systolic_str = parts[0].strip()
        if systolic_str == '':
            systolic = None
        else:
            try:
                systolic = float(systolic_str)
            except ValueError:
                systolic = None

        diastolic_str = parts[1].strip() if len(parts) > 1 else ''
        if diastolic_str == '':
            diastolic = None
        else:
            try:
                diastolic = float(diastolic_str)
            except ValueError:
                diastolic = None
        
        is_invalid = (systolic is None or diastolic is None)
        print(f"    Parsed BP: Systolic={systolic}, Diastolic={diastolic}")
        return systolic, diastolic, is_invalid

    return None, None, True


# BP calculation function
def calculate_bp_risk(systolic, diastolic):
    # Check if missing
    if systolic is None or diastolic is None:
        print("invalid bp")
        return 0, True
    
    # Check if invalid
    if not isinstance(systolic, (int, float)) or not isinstance(diastolic, (int, float)):
        print("invalid bp")
        return 0, True

    # Check if negative/zero
    if systolic <= 0 or diastolic <= 0:
        print("invalid bp")
        return 0, True
    
    # Stage 2
    if systolic >= 140 or diastolic >= 90:
        print("Stage 2 BP 3 points")
        return 3, False

    # Stage 1
    if (130 <= systolic <= 139) or (80 <= diastolic <= 89):
        print("Stage 1 BP 2 points")
        return 2, False

    # Elevated
    if 120 <= systolic <= 129 and diastolic < 80:
        print("Elevated BP 1 point")
        return 1, False

    # Normal
    if systolic < 120 and diastolic < 80:
        print("Normal BP 0 points")
        return 0, False

    return 0, False


# temperature calculation function
def calculate_temp_risk(temperature):
    # Check if missing
    if temperature is None:
        print("Invalid temp")
        return 0, True
    
    # Check if invalid
    if not isinstance(temperature, (int, float)):
        print("Invalid temp")
        return 0, True
    
    # Check if negative/zero
    if temperature <= 0:
        print("Invalid temp")
        return 0, True
    
    # Normal temperature
    if temperature <= 99.5:
        print("Normal temperature 0 points")
        return 0, False
    
    # Low fever
    if 99.6 <= temperature <= 100.9:
        print("Low fever 1 point")
        return 1, False

    # High fever
    if temperature >= 101.0:
        print("High fever 2 points")
        return 2, False
    
    return 0, False


# Age calculation function
def calculate_age_risk(age):
    # Check if missing
    if age is None:
        print("invalid age")
        return 0, True
    
    # Check if invalid
    if not isinstance(age, (int, float)):
        print("invalid age")
        return 0, True
    
    # Check if negative or unrealistic
    if age < 0 or age > 150:
        print("invalid age")
        return 0, True
    
    # Under 40
    if age < 40:
        print("Under 40 - 0 points")
        return 0, False
    
    # 40-65
    if 40 <= age <= 65:
        print("40-65 years - 1 point")
        return 1, False

    # Over 65
    if age > 65:
        print("Over 65 - 2 points")
        return 2, False
    
    return 0, False


# fetch data
def fetch_all_patients():
    all_patients = []
    page = 1

    MAX_RETRIES = 5
    RETRY_DELAY = 2
    REQUEST_DELAY = 0.5
    
    while True:
        url = f"{BASE_URL}/patients?page={page}&limit=20"
        print(f"\nFetching page {page}")
        retry_count = 0
        success = False
        
        while retry_count < MAX_RETRIES and not success:
            try:
                response = requests.get(url, headers=headers, timeout=10)

                if response.status_code == 200:
                    success = True
                    
                elif response.status_code == 429:
                    retry_count += 1
                    wait_time = RETRY_DELAY * (2 ** retry_count)
                    time.sleep(wait_time)
                    continue
                    
                elif response.status_code in [500, 503]:
                    retry_count += 1
                    wait_time = RETRY_DELAY * retry_count
                    time.sleep(wait_time)
                    continue
                    
                else:
                    print(f"Error: status code {response.status_code}")
                    break
                
                if success:
                    try:
                        data = response.json()
                        print(data)
                    except json.JSONDecodeError as e:
                        print(f"JSON parsing error: {str(e)}")
                        retry_count += 1
                        if retry_count < MAX_RETRIES:
                            time.sleep(RETRY_DELAY)
                            success = False
                            continue
                        else:
                            break
                    
                    patients = []
                    
                    # Handle different response formats
                    if isinstance(data, dict):
                        patients = data.get("data", [])
                    elif isinstance(data, list):
                        patients = data

                    print(f"Retrieved {len(patients)} patients")

                    if len(patients) == 0:
                        print("No more patients to fetch")
                        return all_patients
                    
                    all_patients.extend(patients)
                    
                    if len(patients) < 20:
                        print("Last page reached")
                        return all_patients
                    
                    time.sleep(REQUEST_DELAY)
                    
                    page += 1
                    break
                    
            except requests.exceptions.Timeout:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    break
                    
            except requests.exceptions.RequestException as e:
                retry_count += 1
                print(f"Network error: {str(e)}")
                if retry_count < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    break
                    
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                import traceback
                traceback.print_exc()
                break
        
        if not success:
            print(f"Stopped fetching at page {page}")
            break

    print(f"\nTotal patients fetched: {len(all_patients)}")
    return all_patients


# process patients
def process_patients(patients):
    high_risk_patients = []
    fever_patients = []
    data_quality_issues = []
    
    for i, patient in enumerate(patients):
        # Get patient id
        patient_id = patient.get("patient_id", "UNKNOWN")
        name = patient.get("name", "Unknown")

        age = patient.get("age")
        if isinstance(age, str):
            try:
                age = float(age)
            except (ValueError, TypeError):
                age = None
        

        temperature = patient.get("temperature")
        if isinstance(temperature, str):
            if temperature in ['TEMP_ERROR', 'N/A', 'n/a', '']:
                temperature = None
            else:
                try:
                    temperature = float(temperature)
                except (ValueError, TypeError):
                    temperature = None
        
        blood_pressure = patient.get("blood_pressure")

        bp_systolic, bp_diastolic, bp_parse_failed = parse_blood_pressure(blood_pressure)
        
        bp_score, bp_invalid = calculate_bp_risk(bp_systolic, bp_diastolic)
        temp_score, temp_invalid = calculate_temp_risk(temperature)
        age_score, age_invalid = calculate_age_risk(age)
        total_risk_score = bp_score + temp_score + age_score

        if total_risk_score >= 4:
            print(f"HIGH RISK PATIENT")
            high_risk_patients.append(patient_id)
        
        if temperature is not None and isinstance(temperature, (int, float)) and temperature >= 99.6:
            print(f"FEVER PATIENT")
            fever_patients.append(patient_id)
        
        has_data_quality_issue = False
        if bp_invalid:
            print(f"Invalid BP")
            has_data_quality_issue = True
        if temp_invalid:
            print(f"Invalid Temperature")
            has_data_quality_issue = True
        if age_invalid:
            print(f"Invalid Age")
            has_data_quality_issue = True
        
        if has_data_quality_issue:
            data_quality_issues.append(patient_id)
    
    return high_risk_patients, fever_patients, data_quality_issues


def main():
    try:
        patients = fetch_all_patients()
        high_risk, fever, data_quality = process_patients(patients)

        post_data = {
            "high_risk_patients": high_risk,
            "fever_patients": fever,
            "data_quality_issues": data_quality
        }
        
        print(json.dumps(post_data, indent=2))
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()