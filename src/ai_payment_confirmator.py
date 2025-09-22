"""
AI Payment Confirmation service using Google's Gemini AI to match receipts with transactions.
"""
import json
import logging
import os
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import google.generativeai as genai
from src.config import config

logger = logging.getLogger(__name__)

class AIPaymentConfirmator:
    """Service for AI-powered payment confirmation and transaction matching."""
    
    def __init__(self):
        """Initialize the AI payment confirmator with Gemini AI."""
        self.api_key = config.GEMINI_API_KEY
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash')
            logger.info("Gemini AI payment confirmator initialized successfully")
        else:
            logger.warning("GEMINI_API_KEY not provided, AI payment confirmation will be disabled")
            self.model = None
    
    def is_available(self) -> bool:
        """Check if the AI payment confirmator is available."""
        return self.model is not None
    
    def _load_system_prompt(self) -> str:
        """Load the system prompt from the JSON file."""
        try:
            prompt_file = os.path.join('ai', 'payment_confirmation.json')
            with open(prompt_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('system_prompt', '')
        except Exception as e:
            logger.error(f"Error loading payment confirmation system prompt: {e}")
            # Fallback prompt
            return (
                "Siz to'lovlarni tasdiqlovchi sun'iy intellekt tizimisiz. Kvitansiya ma'lumotlarini miqdor, vaqt va karta ma'lumotlariga asoslanib kutilayotgan tranzaktsiyalar bilan moslashtiring. "
                "Faqat yaroqli JSON bilan javob bering: "
                "{'confirm': true/false, 'payment_id': X, 'transaction_id': Y (agar mos kelsa), 'reason': 'tushuntirish'}"
            )
    
    async def confirm_payment(self, payment_data: Dict[str, Any], pending_transactions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Use AI to confirm a payment by matching it with pending transactions.
        
        Args:
            payment_data: Dict containing payment info and receipt data
            pending_transactions: List of pending transaction records
            
        Returns:
            Dict containing AI confirmation response or None if processing failed
        """
        if not self.model:
            logger.error("Gemini AI model not initialized - missing API key")
            return None
        
        try:
            # Prepare the input data for AI
            ai_input = {
                "receipt": {
                    "payment_id": payment_data.get('payment_id'),
                    "amount": payment_data.get('amount'),
                    "transaction_time": payment_data.get('transaction_time', ''),
                    "card_number": payment_data.get('card_number', ''),
                    "merchant_name": payment_data.get('merchant_name', ''),
                    "transaction_id": payment_data.get('transaction_id', '')
                },
                "pending_transactions": []
            }
            
            # Add transaction details
            for txn in pending_transactions:
                ai_input["pending_transactions"].append({
                    "id": txn.get('id'),
                    "amount": txn.get('amount'),
                    "transaction_time": txn.get('transaction_time', ''),
                    "card_balance": txn.get('card_balance'),
                    "card_id": txn.get('card_id'),
                    "raw_message": txn.get('raw_message', '')[:200] if txn.get('raw_message') else ''  # Truncate for AI
                })
            
            # Get the system prompt
            system_prompt = self._load_system_prompt()
            
            # Create the full prompt
            prompt = f"""{system_prompt}

Input data to analyze:
{json.dumps(ai_input, indent=2, ensure_ascii=False)}

Please analyze the receipt data against the pending transactions and provide confirmation response:"""
            
            logger.debug(f"Sending payment confirmation request to AI for payment {payment_data.get('payment_id')}")
            
            # Generate content using Gemini AI
            response = self.model.generate_content(prompt)
            
            if not response.text:
                logger.error("Empty response from Gemini AI for payment confirmation")
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
            
            # Parse the JSON response
            try:
                confirmation_result = json.loads(response_text)
                logger.info(f"AI confirmation result for payment {payment_data.get('payment_id')}: {confirmation_result}")
                return confirmation_result
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response from Gemini AI: {response_text}")
                logger.error(f"JSON parse error: {e}")
                return None
        
        except Exception as e:
            logger.error(f"Error during AI payment confirmation: {e}")
            return None
    
    async def confirm_payment_with_retry(self, payment_data: Dict[str, Any], pending_transactions: List[Dict[str, Any]], retry_delay_minutes: int = 2) -> Optional[Dict[str, Any]]:
        """
        Confirm payment with retry logic as specified.
        
        Args:
            payment_data: Payment data to confirm
            pending_transactions: List of pending transactions
            retry_delay_minutes: Minutes to wait before retry (default 2)
            
        Returns:
            Final confirmation result or None
        """
        try:
            # First attempt
            logger.info(f"First AI confirmation attempt for payment {payment_data.get('payment_id')}")
            result = await self.confirm_payment(payment_data, pending_transactions)
            
            if result and result.get('confirm', False):
                logger.info(f"Payment {payment_data.get('payment_id')} confirmed on first attempt")
                return result
            
            if result and not result.get('confirm', False):
                logger.info(f"Payment {payment_data.get('payment_id')} not confirmed on first attempt. Reason: {result.get('reason', 'Unknown')}")
                logger.info(f"Waiting {retry_delay_minutes} minutes before retry...")
                
                # Wait for retry delay
                await asyncio.sleep(retry_delay_minutes * 60)
                
                # Retry attempt - fetch fresh pending transactions as new ones may have arrived
                logger.info(f"Second AI confirmation attempt for payment {payment_data.get('payment_id')}")
                retry_result = await self.confirm_payment(payment_data, pending_transactions)
                
                if retry_result:
                    if retry_result.get('confirm', False):
                        logger.info(f"Payment {payment_data.get('payment_id')} confirmed on retry attempt")
                    else:
                        logger.info(f"Payment {payment_data.get('payment_id')} still not confirmed on retry. Reason: {retry_result.get('reason', 'Unknown')}")
                    return retry_result
                else:
                    logger.error(f"Retry attempt failed for payment {payment_data.get('payment_id')}")
                    return result  # Return original result
            
            return result
            
        except Exception as e:
            logger.error(f"Error in payment confirmation with retry for payment {payment_data.get('payment_id')}: {e}")
            return None

# Create a global AI payment confirmator instance
ai_payment_confirmator = AIPaymentConfirmator()
