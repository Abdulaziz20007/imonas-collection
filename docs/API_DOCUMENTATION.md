# API Documentation

## 1. Overview

This document outlines the RESTful API for the Order Management System's admin panel.

- **Base URL**: `/api`
- **Authentication**: All endpoints require an authenticated admin session (JWT cookie).
- **Roles**:
  - `admin`: Can manage collections, cards, and view data.
  - `superadmin`: Has all `admin` permissions plus can manage other web panel admin accounts.
- **Data Format**: All requests and responses are in JSON format.
- **Error Handling**: Standard HTTP status codes are used to indicate success or failure.
  - `200 OK`: Request successful.
  - `201 Created`: Resource created successfully.
  - `204 No Content`: Request successful, no response body.
  - `400 Bad Request`: Invalid request payload or parameters.
  - `401 Unauthorized`: Authentication failed or not provided.
  - `403 Forbidden`: Authenticated user does not have permission.
  - `404 Not Found`: Resource not found.
  - `500 Internal Server Error`: Server-side error.

---

## 2. Collection Management

Base Path: `/api/admin/collections`

### `POST /`

- **Description**: Creates a new collection. This action automatically closes the currently active collection.
- **Permission**: `admin`
- **Request Body**: None
- **Success Response**: `201 Created`
  ```json
  {
    "id": 124,
    "status": "open",
    "created_at": "2024-07-29T10:00:00Z",
    "user_count": 0,
    "order_count": 0
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If there are already closed collections that need to be finished.
  - `403 Forbidden`
  - `500 Internal Server Error`

### `POST /{id}/reopen`

- **Description**: Reopens a 'closed' collection. This will merge the current 'open' collection into it and then delete the old 'open' one.
- **Permission**: `admin`
- **URL Parameters**:
  - `id` (integer, required): The ID of the collection to reopen.
- **Request Body**: None
- **Success Response**: `200 OK`
  ```json
  {
    "id": 123,
    "status": "open",
    "created_at": "2024-07-28T10:00:00Z",
    "user_count": 50,
    "order_count": 200
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If the collection is 'finished' or already 'open'.
  - `404 Not Found`
  - `500 Internal Server Error`

### `POST /{id}/finish`

- **Description**: Marks a 'closed' collection as 'finished'. This is a final state.
- **Permission**: `admin`
- **URL Parameters**:
  - `id` (integer, required): The ID of the collection to finish.
- **Request Body**: None
- **Success Response**: `200 OK`
  ```json
  {
    "id": 123,
    "status": "finish",
    "created_at": "2024-07-28T10:00:00Z",
    "finish_at": "2024-07-29T11:00:00Z",
    "user_count": 50,
    "order_count": 200
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If the collection is not 'closed'.
  - `404 Not Found`

### `GET /`

- **Description**: Retrieves a list of all collections with their statistics.
- **Permission**: `admin`
- **Success Response**: `200 OK`
  ```json
  [
    {
      "id": 124,
      "status": "open",
      "created_at": "2024-07-29T10:00:00Z",
      "close_at": null,
      "finish_at": null,
      "user_count": 0,
      "order_count": 0
    },
    {
      "id": 123,
      "status": "closed",
      "created_at": "2024-07-28T10:00:00Z",
      "close_at": "2024-07-29T10:00:00Z",
      "finish_at": null,
      "user_count": 50,
      "order_count": 200
    }
  ]
  ```

---

## 3. Card Management

Base Path: `/api/admin/cards`

### `GET /`

- **Description**: Retrieves a list of all payment cards.
- **Permission**: `admin`
- **Success Response**: `200 OK`
  ```json
  [
    {
      "id": 1,
      "name": "Asosiy Karta",
      "number": "8600123456789012",
      "is_active": true,
      "created_at": "2024-07-29T09:00:00Z"
    }
  ]
  ```

### `POST /`

- **Description**: Adds a new payment card.
- **Permission**: `admin`
- **Request Body**:
  ```json
  {
    "name": "Yangi Karta",
    "number": "8600111122223333"
  }
  ```
- **Validation**:
  - `name`: string, required, max 50 chars.
  - `number`: string, required, 16 digits.
- **Success Response**: `201 Created`
  ```json
  {
    "id": 3,
    "name": "Yangi Karta",
    "number": "8600111122223333",
    "is_active": false,
    "created_at": "2024-07-29T12:00:00Z"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If validation fails or card number already exists.

### `PUT /{id}`

- **Description**: Edits an existing card's details.
- **Permission**: `admin`
- **URL Parameters**:
  - `id` (integer, required): The ID of the card to edit.
- **Request Body**:
  ```json
  {
    "name": "Yangilangan Karta Nomi",
    "number": "8600111122224444"
  }
  ```
- **Validation**:
  - `name`: string, optional, max 50 chars.
  - `number`: string, optional, 16 digits.
- **Success Response**: `200 OK`
  ```json
  {
    "id": 3,
    "name": "Yangilangan Karta Nomi",
    "number": "8600111122224444",
    "is_active": false,
    "created_at": "2024-07-29T12:00:00Z"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If validation fails or card number already exists.
  - `404 Not Found`

### `POST /{id}/activate`

- **Description**: Activates a card, which automatically deactivates all other cards.
- **Permission**: `admin`
- **URL Parameters**:
  - `id` (integer, required): The ID of the card to activate.
- **Request Body**: None
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Card 3 activated successfully."
  }
  ```
- **Error Responses**:
  - `404 Not Found`

### `DELETE /{id}`

- **Description**: Deletes a card. The currently active card cannot be deleted.
- **Permission**: `admin`
- **URL Parameters**:
  - `id` (integer, required): The ID of the card to delete.
- **Success Response**: `204 No Content`
- **Error Responses**:
  - `400 Bad Request`: If trying to delete the active card or the only card.
  - `404 Not Found`

---

## 4. Telegram Admin Management

Base Path: `/api/telegram-admins`

### `GET /`

- **Description**: Retrieves a list of all Telegram bot admins.
- **Permission**: `admin`
- **Success Response**: `200 OK`
  ```json
  [
    {
      "id": "6388760663",
      "name": "Default Admin"
    }
  ]
  ```

### `POST /`

- **Description**: Adds a new Telegram bot admin.
- **Permission**: `admin`
- **Request Body**: `x-www-form-urlencoded`
  - `admin_id` (string, required): The Telegram user ID.
  - `admin_name` (string, required): The name of the admin.
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Admin muvaffaqiyatli qo'shildi"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If validation fails or ID already exists.

### `PUT /{admin_id}`

- **Description**: Edits a Telegram admin's name.
- **Permission**: `admin`
- **URL Parameters**:
  - `admin_id` (string, required): The Telegram user ID of the admin to edit.
- **Request Body**: `x-www-form-urlencoded`
  - `admin_name` (string, required): The new name for the admin.
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Admin muvaffaqiyatli yangilandi"
  }
  ```
- **Error Responses**:
  - `404 Not Found`

### `DELETE /{admin_id}`

- **Description**: Deletes a Telegram bot admin.
- **Permission**: `admin`
- **URL Parameters**:
  - `admin_id` (string, required): The Telegram user ID of the admin to delete.
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Admin muvaffaqiyatli o'chirildi"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If trying to delete the last admin.
  - `404 Not Found`

---

## 5. AI Model & Settings Management

Base Path: `/api/settings`

### `GET /ai/models` (Conceptual)

- **Description**: Retrieves a list of available AI models and the current default. This is part of the general settings response.
- **Permission**: `admin`
- **Success Response**: `200 OK` (as part of `/settings/telegram` page data)

### `POST /telegram`

- **Description**: Updates various Telegram-related settings, including the default AI model.
- **Permission**: `admin`
- **Request Body**: `x-www-form-urlencoded`
  - `default_ai_model` (string, required): e.g., "gemini-2.5-flash-lite".
  - Other settings keys...
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Telegram settings updated successfully"
  }
  ```
- **Error Responses**:
  - `400 Bad Request`: If model name is invalid.

---

## 6. Userbot Management

Base Path: `/api/userbot`

### `GET /status`

- **Description**: Retrieves the current status of the Userbot client.
- **Permission**: `admin`
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "status": {
      "connected": true,
      "authorized": true,
      "state": "AUTHORIZED",
      "me": { "id": 123, "first_name": "Userbot", "phone": "+998901234567" }
    }
  }
  ```

### `POST /send-code`

- **Description**: Initiates the login process by sending a verification code.
- **Permission**: `admin`
- **Request Body**: `x-www-form-urlencoded`
  - `userbot_phone_number` (string, required)
  - `userbot_password` (string, optional): 2FA password.
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Verification code sent successfully",
    "state": "AWAITING_CODE"
  }
  ```

### `POST /verify-code`

- **Description**: Submits the verification code to complete the login.
- **Permission**: `admin`
- **Request Body**: `x-www-form-urlencoded`
  - `code` (string, required): The 5-digit code.
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Userbot connected successfully",
    "state": "AUTHORIZED"
  }
  ```

### `POST /logout`

- **Description**: Logs out the userbot and deletes the session file.
- **Permission**: `admin`
- **Success Response**: `200 OK`
  ```json
  {
    "success": true,
    "message": "Userbot logged out and session cleaned successfully."
  }
  ```

### `POST /clean-session`

- **Description**: Forcefully cleans all userbot session files.
- **Permission**: `admin`
- **Success Response**: `200 OK`
  ```json
  { "success": true, "message": "Session force cleaned successfully." }
  ```
