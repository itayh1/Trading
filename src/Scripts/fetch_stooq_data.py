import time
from io import BytesIO

import numpy as np
import requests
from PIL import Image, ImageEnhance, ImageFilter
import easyocr
from PIL.ImageFile import ImageFile


def _answer_captcha_auto(img: ImageFile) -> str:

    # Step 2: Preprocess the image for better OCR accuracy
    # Convert to grayscale
    gray_img = img.convert("L")

    # Enhance the image (optional, depending on the CAPTCHA's quality)
    enhancer = ImageEnhance.Contrast(gray_img)
    enhanced_img = enhancer.enhance(2)

    # Apply a threshold to convert the image to black and white (binarization)
    bw_img = enhanced_img.point(lambda x: 0 if x < 140 else 255, '1')

    # Optionally apply filtering (e.g., to remove noise)
    filtered_img = bw_img.filter(ImageFilter.MedianFilter())
    filtered_img = np.array(filtered_img)
    filtered_img = np.uint8(filtered_img * 255)
    # Step 3: extract text from the image
    capital_letters_and_numbers = [chr(c) for c in range(ord('A'), ord('Z') + 1)] + [str(i) for i in range(10)]
    lower_letters_and_non_captcha = [chr(c) for c in range(ord('A'), ord('Z') + 1)] + ['@', '#', '$', '&']
    reader = easyocr.Reader(['en'])
    results = reader.readtext(filtered_img, allowlist=capital_letters_and_numbers,
                              blocklist=lower_letters_and_non_captcha)
    print(results)
    results = reader.readtext(np.array(gray_img), allowlist=capital_letters_and_numbers,
                              blocklist=lower_letters_and_non_captcha)
    print(results)
    results = reader.readtext(np.array(enhanced_img), allowlist=capital_letters_and_numbers,
                              blocklist=lower_letters_and_non_captcha)
    print(results)

    # for (bbox, text, prob) in results:
    #     print(f'Detected text: "{text}" with confidence {prob:.2f}')
    # Step 4: Clean the extracted text (CAPTCHAs often have extra whitespace, non-alphanumeric characters)
    # captcha_text = captcha_text.strip()
    # captcha_text = "".join(filter(str.isalnum, captcha_text))  # Remove non-alphanumeric characters

def _answer_captcha_manual(img: ImageFile):
    print('')
    img.show()
    answer = input('Please insert captcha: ')
    return answer.strip()

def get_stooq_data():
    epoch_time = str(time.time())
    epoch_time = epoch_time.split('.')[0] + epoch_time.split('.')[1][:3]

    url = f'https://stooq.com/q/l/s/i/?{epoch_time}='
    # response = requests.get(url)
    # if response.status_code != 200:
    #     raise Exception(f'Request failed with status code {response.status_code}')
    # example_image = Image.open(BytesIO(response.content))
    import base64
    example_image = b'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'
    example_image = base64.b64decode(example_image)
    img = Image.open(BytesIO(example_image))

    answer = _answer_captcha_manual(img)

    requests.post('') #TODO: complete process

def main():
    get_stooq_data()

if __name__ == '__main__':
    main()