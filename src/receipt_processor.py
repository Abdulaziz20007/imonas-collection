"""
Receipt processing service using Google's Gemini AI to extract payment data from images.
"""
import json
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime
import google.generativeai as genai
from PIL import Image
from src.config import config

logger = logging.getLogger(__name__)

class ReceiptProcessor:
    """Service for processing receipt images to extract payment information."""
    
    def __init__(self):
        """Initialize the receipt processor with Gemini AI."""
        self.api_key = config.GEMINI_API_KEY
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash')
            logger.info("Gemini AI receipt processor initialized successfully")
        else:
            logger.warning("GEMINI_API_KEY not provided, receipt processing will be disabled")
            self.model = None
    
    def _load_system_prompt(self) -> str:
        """Load the system prompt from the JSON file."""
        try:
            base_dir = os.path.dirname(__file__)
            prompt_file = os.path.join(base_dir, 'ai', 'receipt2json.json')
            logger.info(f"Attempting to load prompt from: {os.path.abspath(prompt_file)}")
            with open(prompt_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('system_prompt', '')
        except Exception as e:
            try:
                logger.error(f"Error loading system prompt from {os.path.abspath(prompt_file)}: {e}")
            except Exception:
                logger.error(f"Error loading system prompt: {e}")
            # Fallback prompt
            return (
                "Siz to'lovlarni tekshirish tizimi uchun kvitansiyalarni tahlil qiluvchi dastursiz. "
                "Ushbu kvitansiya tasvirini tahlil qiling va to'lov ma'lumotlarini ajratib oling. "
                "Faqat yaroqli JSON bilan javob bering, unda quyidagilar bo'lishi kerak: amount, transaction_time, "
                "card_number, merchant_name, transaction_id, payment_method."
            )

    def _parse_amount(self, amount_value) -> Optional[float]:
        """Parse amount from various locale formats to float.

        Supports inputs like: "5000,00", "5 000,00", "5.000,00", "5,000.00", "5000".
        Returns None if parsing fails.
        """
        # Fast-path for numeric types
        if isinstance(amount_value, (int, float)):
            return float(amount_value)

        if amount_value is None:
            return None

        try:
            import re

            value_str = str(amount_value).strip()
            # Normalize spaces (including non-breaking space) and remove them
            value_str = value_str.replace('\u00A0', ' ').replace(' ', '')
            # Keep only digits, minus, comma, dot
            value_str = re.sub(r"[^0-9\-,\.]", "", value_str)

            if not value_str:
                return None

            # If both separators present, decide decimal by the rightmost one
            if '.' in value_str and ',' in value_str:
                if value_str.rfind(',') > value_str.rfind('.'):
                    # Decimal comma: remove thousands dots, convert comma to dot
                    value_str = value_str.replace('.', '')
                    value_str = value_str.replace(',', '.')
                else:
                    # Decimal dot: remove thousands commas
                    value_str = value_str.replace(',', '')
            else:
                # Only one of them present or none
                if ',' in value_str:
                    idx = value_str.rfind(',')
                    decimals = len(value_str) - idx - 1
                    # Treat 1-2 digits after comma as decimals; otherwise drop commas
                    if 1 <= decimals <= 2:
                        value_str = value_str.replace(',', '.')
                    else:
                        value_str = value_str.replace(',', '')
                elif '.' in value_str:
                    idx = value_str.rfind('.')
                    decimals = len(value_str) - idx - 1
                    # Treat 1-2 digits after dot as decimals; otherwise drop dots (thousands)
                    if not (1 <= decimals <= 2):
                        value_str = value_str.replace('.', '')

            return float(value_str)
        except Exception:
            return None
    
    async def process_receipt_image(self, image_path: str) -> Optional[Dict[str, Any]]:
        """
        Process a receipt image and extract payment data using Gemini AI.
        
        Args:
            image_path: Path to the receipt image file
            
        Returns:
            Dict containing extracted payment data or None if processing failed
        """
        if not self.model:
            logger.error("Gemini AI model not initialized - missing API key")
            return None
        
        try:
            # Validate image file exists
            if not os.path.exists(image_path):
                logger.error(f"Receipt image not found: {image_path}")
                return None
            
            # Load and process the image
            image = Image.open(image_path)
            
            # Get the system prompt
            system_prompt = self._load_system_prompt()
            
            # Create the full prompt
            prompt = f"{system_prompt}\n\nPlease analyze this receipt image and extract the payment information as JSON:"
            
            # Generate content using Gemini AI
            response = self.model.generate_content([prompt, image])
            
            if not response.text:
                logger.error("Empty response from Gemini AI")
                return None
            
            # Clean the response text to extract JSON
            response_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            elif response_text.startswith('```'):
                response_text = response_text[3:]
            
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            
            response_text = response_text.strip()
            # Log raw AI response for troubleshooting (debug level)
            logger.debug(f"Raw AI response: {response_text}")
            
            # Parse the JSON response
            try:
                receipt_data = json.loads(response_text)
                
                # Validate required fields
                required_fields = ['amount', 'transaction_time']
                for field in required_fields:
                    if field not in receipt_data:
                        logger.warning(f"Missing required field '{field}' in receipt data")
                
                # Validate and convert amount to float
                if 'amount' in receipt_data:
                    logger.debug(f"Raw amount value before parsing: {receipt_data.get('amount')}")
                    parsed_amount = self._parse_amount(receipt_data.get('amount'))
                    if parsed_amount is None:
                        logger.error(f"Invalid amount format: {receipt_data.get('amount')}")
                        return None
                    receipt_data['amount'] = parsed_amount
                
                # Validate transaction time format
                if 'transaction_time' in receipt_data:
                    try:
                        # Try to parse the datetime to validate format
                        datetime.strptime(receipt_data['transaction_time'], "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        # Try alternative format without seconds
                        try:
                            datetime.strptime(receipt_data['transaction_time'], "%Y-%m-%d %H:%M")
                        except ValueError:
                            logger.warning(f"Invalid transaction_time format: {receipt_data.get('transaction_time')}")
                
                logger.info(f"Successfully processed receipt from {image_path}")
                logger.debug(f"Extracted data: {receipt_data}")
                
                return receipt_data
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response from Gemini: {e}")
                logger.debug(f"Raw response: {response_text}")
                return None
            
        except Exception as e:
            logger.error(f"Error processing receipt image {image_path}: {e}")
            return None
    
    def is_available(self) -> bool:
        """Check if the receipt processor is available (API key configured)."""
        return self.model is not None

# Create a global instance
receipt_processor = ReceiptProcessor()
