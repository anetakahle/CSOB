import csv
from tavily import TavilyClient
from groq import Groq
import os
from pydantic import BaseModel, Field
import instructor
import random
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict
import time
import json
from pathlib import Path
from enum import Enum
import google.generativeai as genai

class Nationality(Enum):
    AF = "AF"
    AX = "AX"
    AL = "AL"
    DZ = "DZ"
    AS = "AS"
    AD = "AD"
    AO = "AO"
    AI = "AI"
    AQ = "AQ"
    AG = "AG"
    AR = "AR"
    AM = "AM"
    AW = "AW"
    AU = "AU"
    AT = "AT"
    AZ = "AZ"
    BS = "BS"
    BH = "BH"
    BD = "BD"
    BB = "BB"
    BY = "BY"
    BE = "BE"
    BZ = "BZ"
    BJ = "BJ"
    BM = "BM"
    BT = "BT"
    BO = "BO"
    BQ = "BQ"
    BA = "BA"
    BW = "BW"
    BV = "BV"
    BR = "BR"
    IO = "IO"
    BN = "BN"
    BG = "BG"
    BF = "BF"
    BI = "BI"
    KH = "KH"
    CM = "CM"
    CA = "CA"
    CV = "CV"
    KY = "KY"
    CF = "CF"
    TD = "TD"
    CL = "CL"
    CN = "CN"
    CX = "CX"
    CC = "CC"
    CO = "CO"
    KM = "KM"
    CG = "CG"
    CD = "CD"
    CK = "CK"
    CR = "CR"
    CI = "CI"
    HR = "HR"
    CU = "CU"
    CW = "CW"
    CY = "CY"
    CZ = "CZ"
    DK = "DK"
    DJ = "DJ"
    DM = "DM"
    DO = "DO"
    EC = "EC"
    EG = "EG"
    SV = "SV"
    GQ = "GQ"
    ER = "ER"
    EE = "EE"
    ET = "ET"
    FK = "FK"
    FO = "FO"
    FJ = "FJ"
    FI = "FI"
    FR = "FR"
    GF = "GF"
    PF = "PF"
    TF = "TF"
    GA = "GA"
    GM = "GM"
    GE = "GE"
    DE = "DE"
    GH = "GH"
    GI = "GI"
    GR = "GR"
    GL = "GL"
    GD = "GD"
    GP = "GP"
    GU = "GU"
    GT = "GT"
    GG = "GG"
    GN = "GN"
    GW = "GW"
    GY = "GY"
    HT = "HT"
    HM = "HM"
    VA = "VA"
    HN = "HN"
    HK = "HK"
    HU = "HU"
    IS = "IS"
    IN = "IN"
    ID = "ID"
    IR = "IR"
    IQ = "IQ"
    IE = "IE"
    IM = "IM"
    IL = "IL"
    IT = "IT"
    JM = "JM"
    JP = "JP"
    JE = "JE"
    JO = "JO"
    KZ = "KZ"
    KE = "KE"
    KI = "KI"
    KP = "KP"
    KR = "KR"
    KW = "KW"
    KG = "KG"
    LA = "LA"
    LV = "LV"
    LB = "LB"
    LS = "LS"
    LR = "LR"
    LY = "LY"
    LI = "LI"
    LT = "LT"
    LU = "LU"
    MO = "MO"
    MK = "MK"
    MG = "MG"
    MW = "MW"
    MY = "MY"
    MV = "MV"
    ML = "ML"
    MT = "MT"
    MH = "MH"
    MQ = "MQ"
    MR = "MR"
    MU = "MU"
    YT = "YT"
    MX = "MX"
    FM = "FM"
    MD = "MD"
    MC = "MC"
    MN = "MN"
    ME = "ME"
    MS = "MS"
    MA = "MA"
    MZ = "MZ"
    MM = "MM"
    NA = "NA"
    NR = "NR"
    NP = "NP"
    NL = "NL"
    NC = "NC"
    NZ = "NZ"
    NI = "NI"
    NE = "NE"
    NG = "NG"
    NU = "NU"
    NF = "NF"
    MP = "MP"
    NO = "NO"
    OM = "OM"
    PK = "PK"
    PW = "PW"
    PS = "PS"
    PA = "PA"
    PG = "PG"
    PY = "PY"
    PE = "PE"
    PH = "PH"
    PN = "PN"
    PL = "PL"
    PT = "PT"
    PR = "PR"
    QA = "QA"
    RE = "RE"
    RO = "RO"
    RU = "RU"
    RW = "RW"
    BL = "BL"
    SH = "SH"
    KN = "KN"
    LC = "LC"
    MF = "MF"
    PM = "PM"
    VC = "VC"
    WS = "WS"
    SM = "SM"
    ST = "ST"
    SA = "SA"
    SN = "SN"
    RS = "RS"
    SC = "SC"
    SL = "SL"
    SG = "SG"
    SX = "SX"
    SK = "SK"
    SI = "SI"
    SB = "SB"
    SO = "SO"
    ZA = "ZA"
    GS = "GS"
    SS = "SS"
    ES = "ES"
    LK = "LK"
    SD = "SD"
    SR = "SR"
    SJ = "SJ"
    SZ = "SZ"
    SE = "SE"
    CH = "CH"
    SY = "SY"
    TW = "TW"
    TJ = "TJ"
    TZ = "TZ"
    TH = "TH"
    TL = "TL"
    TG = "TG"
    TK = "TK"
    TO = "TO"
    TT = "TT"
    TN = "TN"
    TR = "TR"
    TM = "TM"
    TC = "TC"
    TV = "TV"
    UG = "UG"
    UA = "UA"
    AE = "AE"
    GB = "GB"
    US = "US"
    UM = "UM"
    UY = "UY"
    UZ = "UZ"
    VU = "VU"
    VE = "VE"
    VN = "VN"
    VG = "VG"
    VI = "VI"
    WF = "WF"
    EH = "EH"
    YE = "YE"
    ZM = "ZM"
    ZW = "ZW"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing.log'),
        logging.StreamHandler()
    ]
)

tavily_client = TavilyClient(api_key="REDACTED")

client = Groq(
    api_key="REDACTED",  )

client = instructor.from_groq(client, mode=instructor.Mode.TOOLS)

genai.configure(api_key="REDACTED")

class ExtractedData(BaseModel):
    nationality: Nationality = Field(
        description="Nationality of the footballist"
    )

class GenaiClient:
    def __init__(self):
        self.chat = genai

    def from_response(self, response_text: str) -> ExtractedData:
        """Pomocná metoda pro konverzi odpovědi na ExtractedData"""
        try:
            nationality = Nationality(response_text.strip().upper())
            return ExtractedData(nationality=nationality)
        except ValueError as e:
            raise ValueError(f"Invalid nationality code: {response_text}")

client = GenaiClient()

class DataProcessor:
    def __init__(self, input_file: str, checkpoint_file: str = 'checkpoint.csv'):
        self.input_file = input_file
        self.checkpoint_file = checkpoint_file
        self.processed_data = self._load_checkpoint()
        self.total_rows = len(self.processed_data) - 1 if self.processed_data else self._count_rows()

    def _count_rows(self) -> int:
        with open(self.input_file, 'r', newline='') as csvfile:
            return sum(1 for _ in csv.reader(csvfile)) - 1  # -1 pro header

    def _load_checkpoint(self) -> List[List[str]]:
        if Path(self.checkpoint_file).exists():
            with open(self.checkpoint_file, 'r', newline='', encoding='utf-8') as csvfile:
                return list(csv.reader(csvfile))
        else:
            # Načtení původních dat, pokud checkpoint neexistuje
            with open(self.input_file, 'r', newline='', encoding='utf-8') as csvfile:
                data = list(csv.reader(csvfile))
                header = data[0]
                if "narodnost" not in header:
                    header.append("narodnost")
                return [header] + [[*row, ""] for row in data[1:]]

    def _save_checkpoint(self, data: List[List[str]]):
        with open(self.checkpoint_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)

    def log_progress(self, current_row: int):
        progress = (current_row / self.total_rows) * 100
        logging.info(f"Zpracováno {current_row}/{self.total_rows} řádků ({progress:.2f}%)")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def process_row(self, row: List, name: str) -> str:
        try:
            response = tavily_client.qna_search(query=f"Nationality of a footballist {name} is?")

            model_name = random.choice([
                "gemini-2.0-flash-exp",
                "gemini-1.5-flash"
            ])

            model = genai.GenerativeModel(model_name)

            allowed_nationalities = [n.value for n in Nationality]

            response_message = model.generate_content(
                contents=[
                    {
                        "parts": [
                {
                    "text": f"""Extract nationality name of a footballist {name} from this information: {response}
                    
You must respond with one of these ISO 3166-1 codes: {allowed_nationalities}

Respond in JSON format with exactly one nationality code."""
                }
            ]
                    }
                ],
                generation_config=genai.types.GenerationConfig(
                    temperature=0,
                    response_mime_type="application/json",
                    response_schema={
                        "type": "OBJECT",
                        "properties": {
                            "nationality": {
                                "type": "STRING",
                                "description": "ISO 3166-1 alpha-2 country code"
                            }
                        },
                        "required": ["nationality"]
                    }
                )
            )

            if response_message.prompt_feedback.block_reason:
                raise Exception(f"Content was blocked: {response_message.prompt_feedback.block_reason}")

            json_response = json.loads(response_message.text)
            nationality = Nationality(json_response["nationality"])
            extracted_data = ExtractedData(nationality=nationality)
            return extracted_data.nationality

        except Exception as e:
            logging.error(f"Chyba při zpracování řádku pro {name}: {str(e)}")
            raise

    def process_file(self):
        try:
            data = self.processed_data
            header = data[0]
            nationality_index = header.index("narodnost") if "narodnost" in header else -1

            for i, row in enumerate(data[1:], 1):
                # Přeskočit již zpracované řádky
                if 0 <= nationality_index < len(row) and row[nationality_index]:
                    continue

                name = row[0]
                try:
                    nationality = self.process_row(row, name)
                    nationality_value = nationality.value

                    # Aktualizace národnosti v řádku
                    if nationality_index >= 0:
                        row[nationality_index] = nationality_value
                    else:
                        row.append(nationality_value)

                    self.log_progress(i)

                    # Ukládání checkpointu každých 10 řádků
                    if i % 10 == 0:
                        self._save_checkpoint(data)

                except Exception as e:
                    logging.error(f"Nepodařilo se zpracovat řádek {i}: {str(e)}")
                    continue

            # Uložení finálních výsledků
            self._save_checkpoint(data)

            # Kopírování výsledků do vstupního souboru
            with open(self.input_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data)

        except Exception as e:
            logging.error(f"Kritická chyba při zpracování souboru: {str(e)}")
            raise


if __name__ == "__main__":
    processor = DataProcessor('data.csv')
    processor.process_file()