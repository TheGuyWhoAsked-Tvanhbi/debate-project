import os
import uuid
import json
import time  # Thêm thư viện time để chờ
from flask import Flask, request, jsonify
from google.cloud import storage
import requests

app = Flask(__name__)

UPLOAD_BUCKET_NAME = os.environ.get('UPLOAD_BUCKET_NAME', 'debate-upload-bucket')
storage_client = storage.Client()
upload_bucket = storage_client.bucket(UPLOAD_BUCKET_NAME)

# URL của dịch vụ debate-cloud-run-job (sẽ được truyền qua biến môi trường)
CLOUD_RUN_JOB_URL = os.environ.get('CLOUD_RUN_JOB_URL')

# URL để lấy token xác thực nội bộ giữa các dịch vụ Cloud Run
# audience phải là URL đầy đủ của dịch vụ Cloud Run Job
METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience='


def get_auth_headers():
    """Lấy token xác thực cho cuộc gọi Cloud Run nội bộ."""
    if not CLOUD_RUN_JOB_URL:
        print("CLOUD_RUN_JOB_URL is not set. Cannot get identity token.")
        return {}

    identity_token_url = f"{METADATA_URL}{CLOUD_RUN_JOB_URL}"
    try:
        response = requests.get(identity_token_url, headers={'Metadata-Flavor': 'Google'})
        response.raise_for_status()
        return {'Authorization': f'Bearer {response.text}'}
    except requests.exceptions.RequestException as e:
        print(f"Error getting identity token: {e}")
        return {}


@app.route('/process-debate', methods=['POST'])
def process_debate():
    if 'audio' not in request.files:
        return jsonify({"error": "No audio file provided"}), 400

    audio_file = request.files['audio']
    if audio_file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if not CLOUD_RUN_JOB_URL:
        return jsonify({"error": "Cloud Run Job URL is not configured."}), 500

    if audio_file:
        file_id = str(uuid.uuid4())
        filename = f"{file_id}.mp3"

        try:
            # 1. Lưu file MP3 vào Cloud Storage
            blob = upload_bucket.blob(filename)
            blob.upload_from_file(audio_file)
            print(f"Audio file {filename} uploaded to {UPLOAD_BUCKET_NAME}")

            # 2. Kích hoạt Cloud Run Job để xử lý (đồng bộ)
            job_payload = {
                "gcs_uri": f"gs://{UPLOAD_BUCKET_NAME}/{filename}",
                "file_id": file_id
            }

            headers = get_auth_headers()
            headers['Content-Type'] = 'application/json'

            # Gọi debate-cloud-run-job trực tiếp
            # Timeout được đặt là 300 giây (5 phút) để chờ Cloud Run Job hoàn thành
            job_response = requests.post(
                CLOUD_RUN_JOB_URL,
                headers=headers,
                data=json.dumps(job_payload),
                timeout=300
            )
            job_response.raise_for_status()  # Ném lỗi nếu có lỗi HTTP

            result_data = job_response.json()

            # 3. Xóa file gốc sau khi xử lý thành công
            blob.delete()
            print(f"Audio file {filename} deleted from {UPLOAD_BUCKET_NAME}")

            return jsonify({"status": "success", "winning_team": result_data.get("winning_team")}), 200

        except requests.exceptions.Timeout:
            print(f"Cloud Run job for {filename} timed out.")
            return jsonify({"error": "Processing timed out. Please try again later."}), 504
        except requests.exceptions.RequestException as e:
            print(f"Error calling Cloud Run job: {e}. Response: {e.response.text if e.response else 'N/A'}")
            return jsonify({"error": f"Failed to process debate: {e}"}), 500
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return jsonify({"error": f"An unexpected error occurred: {e}"}), 500

    return jsonify({"error": "Invalid request"}), 400


if __name__ == '__main__':
    # Chỉ chạy trên môi trường local để test
    # Đảm bảo CLOUD_RUN_JOB_URL được đặt trong biến môi trường cục bộ
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
