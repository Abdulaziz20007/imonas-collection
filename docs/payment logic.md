graph TD
subgraph "1-Oqim: Foydalanuvchi To'lov Yuborishi (Kechiktirilgan Tekshiruv bilan)"
A1(Boshlanish: Foydalanuvchi kvitansiya rasmini yuboradi) --> A2[Bot kvitansiyani yuklab oladi];
A2 --> A3["SI (receipt_processor):<br>Rasmdan tuzilmali JSON ma'lumotlarni ajratib oladi"];
A3 --> A4{Ma'lumotlar muvaffaqiyatli ajratildimi?};
A4 -- Yo'q --> A5[Foydalanuvchini xatolik haqida xabardor qilish];
A5 --> A6[Admin tomonidan qo'lda tekshirish uchun belgilash] --> End1(Tugatish);
A4 -- Ha --> A7["MB: 'payment' yozuvini yaratish<br>status = 'pending'"];
A7 --> A8["<b>1 daqiqa kutish</b><br>(Bank xabarini kutish uchun asinxron vazifa)"];
A8 --> C1(Tekshiruvni ishga tushirish);
end

    subgraph "2-Oqim: Bank Tranzaksiyalarini Kuzatuvchi (Avtomatlashtirilgan)"
        B1(Boshlanish: Userbot bank botini kuzatadi) --> B2{Hisobni to'ldirish haqida yangi xabar keldimi?};
        B2 -- Ha --> B3[Xabarni tahlil qilish: miqdor, vaqt, karta raqami oxiri];
        B3 --> B4["MB: Karta raqami oxiriga mos keluvchi faol kartani topish"];
        B4 --> B5{Karta topildimi?};
        B5 -- Ha --> B6["MB: 'transaction' yozuvini yaratish<br>is_done = 0"] --> B_End(Tugatish);
        B5 -- Yo'q --> B7["'Noma'lum Karta' xatosini yozib qo'yish"] --> B_End;
        B2 -- Yo'q --> B1;
    end

    subgraph "3-Oqim: Sun'iy Intellektga Asoslangan Tekshiruv Mantig'i"
        C1 --> C2[Kutilayotgan to'lov va kvitansiya ma'lumotlarini olish];
        C2 --> C3[MB'dan tegishli tranzaksiyalarni olish (to'lov vaqti atrofida)];
        C3 --> C4{"SI (ai_payment_confirmator):<br>Kvitansiya ma'lumotlarini tranzaksiya bilan moslashtirish"};
        C4 -- "Moslik Topildi<br>(confirm: true)" --> C5["MB: To'lov holatini 'verified' ga o'zgartirish"];
        C5 --> C6["MB: To'lovni tranzaksiyaga bog'lash"];
        C6 --> C7["MB: Tranzaksiyani 'done' deb belgilash"];
        C7 --> C8["MB: Foydalanuvchining payment_step'ini 'confirmed' ga o'zgartirish"];
        C8 --> C9[Foydalanuvchini Xabardor Qilish: To'lov muvaffaqiyatli, taklifnoma yuborish];
        C9 --> C10[Adminni Xabardor Qilish: SI tekshiruvi muvaffaqiyatli] --> End2(Tugatish);
        C4 -- "Moslik Topilmadi<br>(confirm: false)" --> C11["MB: To'lov holatini 'manual_review' ga o'zgartirish"];
        C11 --> C12[Adminni Xabardor Qilish: To'lovni qo'lda tekshirish kerak] --> End2;
    end
