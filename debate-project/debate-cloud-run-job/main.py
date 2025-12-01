import os
import json
from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import speech_v1p1beta1 as speech

app = Flask(__name__)

STORAGE_CLIENT = storage.Client()
PROCESSING_BUCKET_NAME = os.environ.get('PROCESSING_BUCKET_NAME', 'debate-processing-bucket')
PROCESSING_BUCKET = STORAGE_CLIENT.bucket(PROCESSING_BUCKET_NAME)

SPEECH_CLIENT = speech.SpeechClient()

@app.route('/', methods=['GET', 'POST']) # Thêm 'GET' để xử lý health checks
def process_debate_job():
    if request.method == 'GET':
        # Đây là phản hồi cho health check của Cloud Run.
        # Ứng dụng chỉ cần trả về một phản hồi HTTP 200 OK.
        return "OK", 200

    # Nếu là POST, chúng ta mong đợi dữ liệu JSON
    try:
        data = request.get_json()
        if not data: # Kiểm tra nếu dữ liệu JSON không có hoặc không hợp lệ
            print("Received POST request without valid JSON data.")
            return jsonify({"error": "No JSON data provided or invalid JSON format"}), 400

        gcs_uri = data.get("gcs_uri")
        file_id = data.get("file_id")

        if not gcs_uri or not file_id:
            return jsonify({"error": "Missing gcs_uri or file_id in payload"}), 400

        print(f"Starting processing for file: {gcs_uri}")

        # ... (Phần còn lại của mã xử lý của bạn không thay đổi) ...
        # 1. Chuyển đổi giọng nói thành văn bản và phân biệt người nói
        audio = {"uri": gcs_uri}

        config = {
            "enable_speaker_diarization": True,
            "diarization_speaker_count": 6,
            "language_code": "vi-VN",
            "encoding": speech.RecognitionConfig.AudioEncoding.MP3,
            "sample_rate_hertz": 44100,
        }

        operation = SPEECH_CLIENT.long_running_recognize(config=config, audio=audio)
        print("Waiting for Speech-to-Text operation to complete...")
        response = operation.result(timeout=120)

        transcript_data = []
        for result in response.results:
            alternative = result.alternatives[0]
            words = []
            for word_info in alternative.words:
                words.append({
                    "word": word_info.word,
                    "speaker_tag": word_info.speaker_tag
                })
            
            transcript_data.append({
                "confidence": alternative.confidence,
                "transcript": alternative.transcript,
                "words": words
            })

        # 2. Xử lý logic chấm điểm (PHẦN BẠN CẦN TÙY CHỈNH RẤT NHIỀU)
        speaker_transcripts = {}
        for item in transcript_data:
            current_speaker = None
            current_segment = []
            for word_info in item['words']:
                if current_speaker is None or current_speaker != word_info['speaker_tag']:
                    if current_segment:
                        speaker_transcripts.setdefault(current_speaker, []).append(" ".join(current_segment))
                    current_speaker = word_info['speaker_tag']
                    current_segment = [word_info['word']]
                else:
                    current_segment.append(word_info['word'])
            if current_segment:
                speaker_transcripts.setdefault(current_speaker, []).append(" ".join(current_segment))
        
        winning_team = 0
        score_details = {}

        if 1 in speaker_transcripts and 2 in speaker_transcripts:
            len_speaker1_words = sum(len(segment.split()) for segment in speaker_transcripts[1])
            len_speaker2_words = sum(len(segment.split()) for segment in speaker_transcripts[2])
            if len_speaker1_words > len_speaker2_words:
                winning_team = 1
            else:
                winning_team = 0
        else:
            winning_team = 0
            
        return jsonify({
            "status": "processed",
            "file_id": file_id,
            "winning_team": winning_team,
            "transcript": transcript_data,
            "speaker_transcripts": speaker_transcripts,
            "score_details": score_details
        }), 200

    except requests.exceptions.Timeout:
        print(f"Speech-to-Text operation timed out for {gcs_uri}.")
        return jsonify({"error": "Speech-to-Text processing timed out."}), 504
    except Exception as e:
        print(f"Error processing debate job for {gcs_uri}: {e}")
        # Ghi log chi tiết lỗi ra console
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to process debate job: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

