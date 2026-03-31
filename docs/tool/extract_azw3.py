"""Proper MOBI/AZW3 text extraction with PalmDoc decompression."""
import struct
import zlib
import re

BOOK_PATH = "/Users/zhuquanmin/Documents/plan/book/A05904. 对冲之王：华尔街量化投资传奇（经典版）.B073WS5HT2.azw3"

def palmdoc_decompress(data):
    """PalmDoc (LZ77) decompression."""
    result = bytearray()
    i = 0
    while i < len(data):
        c = data[i]
        i += 1
        if c == 0:
            result.append(0)
        elif 1 <= c <= 8:
            result.extend(data[i:i+c])
            i += c
        elif c <= 0x7F:
            result.append(c)
        elif c <= 0xBF:
            if i < len(data):
                next_byte = data[i]
                i += 1
                distance = ((c << 8) | next_byte) >> 3 & 0x7FF
                length = (next_byte & 0x07) + 3
                for _ in range(length):
                    result.append(result[-distance])
            else:
                break
        else:
            result.append(ord(' '))
            result.append(c ^ 0x80)
    return bytes(result)

with open(BOOK_PATH, 'rb') as f:
    data = f.read()

# Parse PalmDB header
num_records = struct.unpack('>H', data[76:78])[0]
records = []
for i in range(num_records):
    offset = 78 + i * 8
    rec_offset = struct.unpack('>I', data[offset:offset+4])[0]
    records.append(rec_offset)

# Parse MOBI header from record 0
rec0 = records[0]
compression = struct.unpack('>H', data[rec0:rec0+2])[0]
text_length = struct.unpack('>I', data[rec0+4:rec0+8])[0]
record_count = struct.unpack('>H', data[rec0+8:rec0+10])[0]
record_size = struct.unpack('>H', data[rec0+10:rec0+12])[0]
encryption = struct.unpack('>H', data[rec0+12:rec0+14])[0]

print(f"Compression: {compression} (1=none, 2=PalmDoc, 17480=HUFF/CDIC)")
print(f"Text length: {text_length}")
print(f"Record count: {record_count}")
print(f"Record size: {record_size}")
print(f"Encryption: {encryption} (0=none, 1=old, 2=DRM)")
print(f"Total records in file: {num_records}")

# Check MOBI header
mobi_header_length = struct.unpack('>I', data[rec0+20:rec0+24])[0] if len(data) > rec0+24 else 0
print(f"MOBI header length: {mobi_header_length}")

# For KF8 (azw3), check for BOUNDARY record
kf8_offset = None
for i in range(len(records)-1, 0, -1):
    rec_start = records[i]
    if rec_start + 8 <= len(data):
        marker = data[rec_start:rec_start+8]
        if marker == b'BOUNDARY':
            kf8_offset = i
            print(f"Found KF8 BOUNDARY at record {i}")
            break

# Try to decompress text records
all_text = bytearray()
start_rec = 1
end_rec = min(record_count + 1, len(records))

print(f"\nDecompressing records {start_rec} to {end_rec-1}...")

for i in range(start_rec, end_rec):
    rec_start = records[i]
    rec_end = records[i+1] if i+1 < len(records) else len(data)
    rec_data = data[rec_start:rec_end]
    
    try:
        if compression == 2:  # PalmDoc
            trail = rec_data[-1] & 0x03
            if trail > 0:
                rec_data = rec_data[:-trail]
            decompressed = palmdoc_decompress(rec_data)
            all_text.extend(decompressed)
        elif compression == 1:  # No compression
            all_text.extend(rec_data)
        elif compression == 17480:  # HUFF/CDIC
            all_text.extend(rec_data)
    except Exception as e:
        pass

# Try to decode
try:
    text = all_text.decode('utf-8', errors='ignore')
except:
    text = all_text.decode('gbk', errors='ignore')

# Strip HTML tags
text = re.sub(r'<[^>]+>', '\n', text)
text = re.sub(r'&nbsp;', ' ', text)
text = re.sub(r'&[a-zA-Z]+;', '', text)
text = re.sub(r'\n{3,}', '\n\n', text)

lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 2]
print(f"\nExtracted {len(lines)} lines of text\n")
for line in lines[:300]:
    print(line)
