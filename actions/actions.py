# This files contains your custom actions which can be used to run
# custom Python code.
#
# See this guide on how to implement these action:
# https://rasa.com/docs/rasa/custom-actions


# This is a simple example for a custom action which utters "Hello World!"

from typing import Any, Text, Dict, List

from rasa_sdk import Action, Tracker, FormValidationAction
from rasa_sdk.events import EventType, SlotSet, SessionStarted, ActionExecuted
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.types import DomainDict

import sqlite3
import pandas as pd

import datetime

connection = sqlite3.connect('actions/rasa_db.db')
cursor = connection.cursor()

cursor.execute('SELECT * FROM master')
all_users = cursor.fetchall()

DF_NAME = pd.DataFrame(all_users,columns=['id', 'name','address'])
ALLOWED_NAME = DF_NAME.name.to_list()

cursor.execute('SELECT * FROM office')
all_users = cursor.fetchall()

DF_ADDRESS = pd.DataFrame(all_users,columns=['id', 'name','address'])
ALLOWED_ADDRESS = DF_ADDRESS.name.to_list()




class ActionSessionStart(Action):
    def name(self) -> Text:
        return "action_session_start"

    async def run(
      self, dispatcher, tracker: Tracker, domain: Dict[Text, Any]
    ) -> List[Dict[Text, Any]]:

        input_data=tracker.latest_message
        print(input_data)

        # telegram_username = input_data["metadata"]["message"]["from"]["first_name"] 
        try:
            telegram_id = input_data["metadata"]["message"]["from"]["id"]
            cursor.execute('SELECT * FROM user WHERE id_telegram = ?',(telegram_id,))
            users = cursor.fetchall()
            if len(users) > 0:
                user_id = users[0][0]
                # есть ли у него записи?
                cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                user_appointment = cursor.fetchall()
                if len(user_appointment) > 0:
                    have_appointment = True
                else:
                    have_appointment = False
                # message=f"Добрый день, {telegram_username}! \nРад новой встрече"
            else:
                # Добавляем нового пользователя
                cursor.execute('INSERT INTO user (id_telegram) VALUES (?)', (telegram_id,))
                connection.commit()
                cursor.execute('SELECT * FROM user WHERE id_telegram = ?',(telegram_id,))
                users = cursor.fetchall()
                user_id = users[0][0]
                have_appointment = False
                # message=f"Добрый день, {telegram_username}! \nЯ чат-бот компании..."            
        except:
            user_id = 0
            have_appointment = False
        # telegram_chat_type = tracker.latest_message["metadata"]["chat"]["type"]
        # if telegram_chat_type == 'group':
        #     is_group = True
        # return [
        #             SlotSet("is_in_telegram_group", is_group),
        #         ]


        
        # dispatcher.utter_message(text=message)        

        metadata = tracker.get_slot("session_started_metadata")
        print(metadata)
        print(user_id)

        return [SessionStarted(), 
                SlotSet("user_id", user_id),
                SlotSet("have_appointment", have_appointment),
                ActionExecuted("action_listen")]


class ActionAppointmentReset(Action): # удалить запись из БД !!!!!
 
    def name(self) -> Text:              
        return "action_appointment_reset"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        # user_id = tracker.get_slot("user_id")
        button_id = tracker.get_slot("button_id")
        # address = tracker.get_slot("address")
        # master  = tracker.get_slot("master")
        # time  = tracker.get_slot("time")
        # if time == None:
        #     today = datetime.datetime.now()
        #     today = today.strftime("%Y-%m-%d %H:%M:%S")
        #     time = today
        # else:
        #     time = str(time)
        #     time = time.replace('T',' ')
        # cursor.execute('SELECT MIN(date) FROM appointment WHERE user = ?',(user_id,))
        # array_appointment = cursor.fetchall()
        cursor.execute('UPDATE appointment SET user = NULL WHERE id_appointment = ?',(int(button_id),))
        connection.commit()
        # time = array_appointment[0][0]
        cursor.execute('SELECT master, date FROM appointment WHERE id_appointment = ?',(int(button_id),))
        user = cursor.fetchall()
        master = str(user[0][0])
        time = str(user[0][1])

        message = f"Ваша запись к мастеру {master} на {time} отменена"
        dispatcher.utter_message(text= message)
        return [SlotSet("address", None),
                SlotSet("master", None),
                SlotSet("time", None),
                SlotSet("button_id", None)]


class ActionAppointmentCheck(Action): # вывести из базы записи !!!!
 
    def name(self) -> Text:            
        return "action_appointment_check"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        user_id = tracker.get_slot("user_id")
        print(f"slot: {user_id}")
        if user_id == None:
                    print(f"no user id: {user_id}")
                    try:
                        input_data=tracker.latest_message
                        telegram_id = input_data["metadata"]["message"]["from"]["id"]
                        cursor.execute('SELECT * FROM user WHERE id_telegram = ?',(telegram_id,))
                        users = cursor.fetchall()
                        if len(users) > 0:
                            user_id = users[0][0]
                            # есть ли у него записи?
                            cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                            user_appointment = cursor.fetchall()
                            if len(user_appointment) > 0:
                                have_appointment = True
                            else:
                                have_appointment = False
                            # message=f"Добрый день, {telegram_username}! \nРад новой встрече"
                        else:
                            # Добавляем нового пользователя
                            cursor.execute('INSERT INTO user (id_telegram) VALUES (?)', (telegram_id,))
                            connection.commit()
                            cursor.execute('SELECT * FROM user WHERE id_telegram = ?',(telegram_id,))
                            users = cursor.fetchall()
                            user_id = users[0][0]
                            have_appointment = False
                    except:
                        pass
        today = datetime.datetime.now()
        today = today.strftime("%Y-%m-%d %H:%M:%S")        
        cursor.execute('SELECT master, date FROM appointment WHERE user = ? and date >= ?',(user_id, today))
        array_appointment = cursor.fetchall()
        if len(array_appointment) != 0:
            dispatcher.utter_message(text = 'вы записаны:')
            for appointment in array_appointment:
                text_appointment = 'к мастеру ' + str(appointment[0]) + ' на ' + str(appointment[1])
                dispatcher.utter_message(text = text_appointment)
        else:
            dispatcher.utter_message(text = 'у вас нет записей')

        return [SlotSet("address", None),
                SlotSet("master", None),
                SlotSet("time", None)]


class ActionAppointmentCheckChoise(Action): # какую запись отменить !!!!
 
    def name(self) -> Text:            
        return "action_appointment_check_choise"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        
        user_id = tracker.get_slot("user_id")
        if user_id == None:
                    try:
                        input_data=tracker.latest_message
                        telegram_id = input_data["metadata"]["message"]["from"]["id"]
                        cursor.execute('SELECT * FROM user WHERE id_telegram = ?',(telegram_id,))
                        users = cursor.fetchall()
                        if len(users) > 0:
                            user_id = users[0][0]
                            # есть ли у него записи?
                            cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                            user_appointment = cursor.fetchall()
                            if len(user_appointment) > 0:
                                have_appointment = True
                            else:
                                have_appointment = False
                            # message=f"Добрый день, {telegram_username}! \nРад новой встрече"
                        else:
                            # Добавляем нового пользователя
                            cursor.execute('INSERT INTO user (id_telegram) VALUES (?)', (telegram_id,))
                            connection.commit()
                            cursor.execute('SELECT * FROM user WHERE id_telegram = ?',(telegram_id,))
                            users = cursor.fetchall()
                            user_id = users[0][0]
                            have_appointment = False
                    except:
                        pass
        # user_id = "57"
        today = datetime.datetime.now()
        today = today.strftime("%Y-%m-%d %H:%M:%S")  
        base_query = 'SELECT master, date, id_appointment FROM appointment WHERE user = ? and date >= ?'
        cursor.execute(base_query,(user_id, today))
        array_appointment = cursor.fetchall()
        if len(array_appointment) != 0:
            buttons = []
            for user in array_appointment:
                title_time = str(user[1])
                # title_name = str(user[0])
                title_id = str(user[2])
                # dispatcher.utter_message(text = str(user[1])+str(user[0]))
                # dispatcher.utter_message(text = title_time[:11])
                payload = '/inform{"button_id":"'+f'{title_id}'+'"}'
                buttons.append({'title':title_time, 'payload':payload})
            # dispatcher.utter_message(text=str(buttons))
            messege = "какую запись отменить"
            dispatcher.utter_message(text=messege,buttons=buttons,button_type="vertical")
        else:
            dispatcher.utter_message(text = 'у вас нет записей')
        
        # return [SlotSet("user_id", "57"),
        #         ActionExecuted("action_listen")]


class ActionDayOff(Action): # вывести из базы свободные дни !!!!
 
    def name(self) -> Text:            
        return "action_day_off"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        address = tracker.get_slot("address")
        master  = tracker.get_slot("master")
        time  = tracker.get_slot("time")

        # dispatcher.utter_message(text = str(time)+' '+str(master)+' '+str(address))

        base_query = 'SELECT master, date FROM appointment WHERE user IS NULL and date >= ?'
        
        if time == None:
            today = datetime.datetime.now()
            today = today.strftime("%Y-%m-%d %H:%M:%S")
            time = today
        else:
            time = str(time)
            time = time.replace('T',' ')
            # dispatcher.utter_message(text = 'запуск action_day_off time')
        
        if master in ALLOWED_NAME: # если есть имя игнорируем адресс
            query = base_query + ' and master = ?'
            cursor.execute(query,(time,master,))
            users = cursor.fetchmany(5)
            # dispatcher.utter_message(text = 'запуск action_day_off cursor')

            # dispatcher.utter_message(text = 'запуск action_day_off')
            # Выводим результаты
            dispatcher.utter_message(text = f'у мастера {master} открыта запись:')
            for user in users:
                text_user = str(user[1])
                dispatcher.utter_message(text = text_user)
        
        else:
            if address in ALLOWED_ADDRESS and master not in ALLOWED_NAME: # если есть только адресс без имени
                # dispatcher.utter_message(text = 'запуск address')
                query = 'SELECT name FROM master WHERE office = ?'
                cursor.execute(query,(address,))
                users = cursor.fetchall()
                names = (pd.DataFrame(users,columns=['name'])).name.to_list()
                # dispatcher.utter_message(text = 'запуск address 2')
                query = 'SELECT master, date FROM appointment WHERE user IS NULL and date > ? and master IN '+str(tuple(names))
                if len(names)==1:
                    query = 'SELECT master, date FROM appointment WHERE user IS NULL and date > ? and master IN '+str(tuple(names))[:-2]+")"
                cursor.execute(query,(time,))
                users = cursor.fetchmany(5)
                dispatcher.utter_message(text = f'в салоне на {address} открыта запись:')
                for user in users:
                    text_user = str(user[1])
                    dispatcher.utter_message(text = text_user)

            else:
                query = base_query
                cursor.execute(query,(time,))
                users = cursor.fetchmany(5)
                # dispatcher.utter_message(text = 'запуск action_day_off cursor')

                # dispatcher.utter_message(text = 'запуск action_day_off')
                # Выводим результаты
                dispatcher.utter_message(text = f'ближайшие свободные записи:')
                for user in users:
                    text_user = str(user[1])
                    dispatcher.utter_message(text = text_user)
 
        return [SlotSet("address", None),
                SlotSet("master", None),
                SlotSet("time", None)]


class ActionAppointmentChoice(Action): # ПРЕДЛОЖЕНИЯ ПО ЗАПИСИ !!!!
 
    def name(self) -> Text:            
        return "action_appointmen_choice"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        address = tracker.get_slot("address")
        master  = tracker.get_slot("master")
        time  = tracker.get_slot("time")
        # user_id = tracker.get_slot("user_id")

        # time = time.replace('T',' ')
        # dispatcher.utter_message(text = str(time)+' '+str(master)+' '+str(address)+' '+str(user_id))

        base_query = 'SELECT master, date, id_appointment FROM appointment WHERE user IS NULL and date >= ?'
        
        if time == None:
            today = datetime.datetime.now()
            today = today.strftime("%Y-%m-%d %H:%M:%S")
            time = today
        else:
            time = str(time)
            time = time[:10]+' '+time[11:19]
            
        # dispatcher.utter_message(text = f'установлено время {time}')
        
        if master in ALLOWED_NAME: # если есть имя игнорируем адресс
            query = base_query + ' and master = ?'
            cursor.execute(query,(time,master,))
            users = cursor.fetchmany(4)
            # dispatcher.utter_message(text = 'запуск action_day_off cursor')

            # dispatcher.utter_message(text = 'запуск action_day_off')
            # Выводим результаты
            if len(users) != 0:
                dispatcher.utter_message(text = f'у мастера {master} ближайшие записи открыты:')
                buttons = []
                for user in users:
                    title_time = str(user[1])
                    # title_name = str(user[0])
                    title_id = str(user[2])
                    # dispatcher.utter_message(text = str(user[1])+str(user[0]))
                    # dispatcher.utter_message(text = title_time[:11])
                    payload = '/inform{"button_id":"'+f'{title_id}'+'"}'
                    buttons.append({'title':title_time, 'payload':payload})
                messege = "выберите удобное время"
                dispatcher.utter_message(text=messege,buttons=buttons,button_type="vertical")
            else:
                dispatcher.utter_message(text = f'у мастера {master} нет свободной записи')
            # return [SlotSet("address", None),
            #         SlotSet("master", None),
            #         SlotSet("time", None)]
        
        elif address in ALLOWED_ADDRESS and master not in ALLOWED_NAME: # если есть только адресс без имени
            # dispatcher.utter_message(text = 'запуск address')
            query = 'SELECT name FROM master WHERE office = ?'
            cursor.execute(query,(address,)) 
            users = cursor.fetchall()
            names = (pd.DataFrame(users,columns=['name'])).name.to_list()
            # dispatcher.utter_message(text = 'запуск address 2')
            if len(names)==1:
                query = base_query + ' and master IN '+str(tuple(names))[:-2]+")"
            else:
                query = base_query + ' and master IN '+str(tuple(names))
            cursor.execute(query,(time,))
            users = cursor.fetchmany(4)
            dispatcher.utter_message(text = f'в салоне на {address} открыта запись:')
            buttons = []
            for user in users:
                title_time = str(user[1])
                # title_name = str(user[0])
                title_id = str(user[2])
                # dispatcher.utter_message(text = str(user[1])+str(user[0]))
                # dispatcher.utter_message(text = title_time[:11])
                payload = '/inform{"button_id":"'+f'{title_id}'+'"}'
                buttons.append({'title':title_time, 'payload':payload})
            messege = "выберите удобное время"
            # dispatcher.utter_message(text=str(buttons))
            dispatcher.utter_message(text=messege,buttons=buttons,button_type="vertical")
            # return [SlotSet("address", None),
            #         SlotSet("master", None),
            #         SlotSet("time", None)]
        
        # base_query = 'SELECT master, date, id_appointment FROM appointment WHERE user IS NULL and date >= ?'
        else:
            query = base_query 
            # dispatcher.utter_message(text = time)
            cursor.execute(query,(time,))
            users = cursor.fetchmany(4)
            # dispatcher.utter_message(text = str(users))

            # Выводим результаты
            buttons = []
            for user in users:
                title_time = str(user[1])
                # title_name = str(user[0])
                title_id = str(user[2])
                # dispatcher.utter_message(text = str(user[1])+str(user[0]))
                # dispatcher.utter_message(text = title_time[:11])
                payload = '/inform{"button_id":"'+f'{title_id}'+'"}'
                buttons.append({'title':title_time, 'payload':payload})
            messege = "выберите удобное время"
            # dispatcher.utter_message(text=str(buttons))
            dispatcher.utter_message(text=messege,buttons=buttons,button_type="vertical")     

            # return [SlotSet("address", None),
            #         SlotSet("master", None),
            #         SlotSet("time", None)]


class ActionAppointmentInset(Action): # ЗАПИСЬ В БД (непосредственное занесение в БД) !!!!
 
    def name(self) -> Text:            
        return "action_appointment_inset"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        # master  = tracker.get_slot("master")
        # time  = tracker.get_slot("time")    
        user_id = tracker.get_slot("user_id")   
        button_id = tracker.get_slot("button_id")

        # dispatcher.utter_message(text= str(button_id))
        cursor.execute('UPDATE appointment SET user = ? WHERE id_appointment = ?',(user_id, int(button_id),))
        connection.commit()
        # dispatcher.utter_message(text= 'внесено в БД')
        cursor.execute('SELECT master, date FROM appointment WHERE id_appointment = ?',(int(button_id),))
        user = cursor.fetchall()
        master = str(user[0][0])
        time = str(user[0][1])

        message = f"Вы записаны к мастеру {master} на {time}"
        dispatcher.utter_message(text= message)
        return [SlotSet("address", None),
                SlotSet("master", None),
                SlotSet("time", None),
                SlotSet("button_id", None)]


class ActionUserID(Action): # Аутентификация !!!!
 
    def name(self) -> Text:            
        return "action_user_id"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        # user_id = tracker.get_slot("user_id")
        # dispatcher.utter_message(text = f'установлен id {user_id}')
        text  = tracker.latest_message['text']
        try:
            c = int(text)
        except:
            dispatcher.utter_message(text = 'неверный формат номера')
            return [SlotSet("user_id", None),
                    SlotSet("have_appointment", None)]
        if c > 999999999 and c < 10000000000:
            cursor.execute('SELECT * FROM user WHERE phone = ?',(c,))
            users = cursor.fetchall()
            if len(users) > 0:
                user_id = users[0][0]
                # есть ли у него записи?
                cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                user_appointment = cursor.fetchall()
                if len(user_appointment) > 0:
                    have_appointment = True
                else:
                    have_appointment = False
            else:
                # Добавляем нового пользователя
                cursor.execute('INSERT INTO user (phone) VALUES (?)', (c,))
                connection.commit()
                cursor.execute('SELECT * FROM user WHERE phone = ?',(c,))
                users = cursor.fetchall()
                if len(users) > 0:
                    user_id = users[0][0]
                    # есть ли у него записи?
                    cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                    user_appointment = cursor.fetchall()
                    if len(user_appointment) > 0:
                        have_appointment = True
                    else:
                        have_appointment = False
        else:
            dispatcher.utter_message(text = 'неверный формат номера')
            return [SlotSet("user_id", None),
                    SlotSet("have_appointment", None)]
        
        return [SlotSet("user_id", user_id),
                SlotSet("have_appointment", have_appointment)]


class ActionID(FormValidationAction):

    def name(self) -> Text:
        return "validate_phone_number_form"

    def validate_phone_number(
        self,
        slot_value: Any,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: DomainDict
        ) -> Dict[Text, Any]:

        text  = slot_value
        # dispatcher.utter_message(text = f'запуск проверки слота {slot_value}')
        try:
            c = int(text)
        except:
            dispatcher.utter_message(text = 'неверный формат номера 1')
            return {"user_id": None, 
                    "phone_number": None, 
                    "have_appointment": None} 
        if c > 999999999 and c < 10000000000:
            cursor.execute('SELECT * FROM user WHERE phone = ?',(c,))
            users = cursor.fetchall()
            if len(users) > 0:
                user_id = users[0][0]
                # есть ли у него записи?
                cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                user_appointment = cursor.fetchall()
                if len(user_appointment) > 0:
                    have_appointment = True
                else:
                    have_appointment = False
            else:
                # Добавляем нового пользователя
                cursor.execute('INSERT INTO user (phone) VALUES (?)', (c,))
                connection.commit()
                cursor.execute('SELECT * FROM user WHERE phone = ?',(c,))
                users = cursor.fetchall()
                if len(users) > 0:
                    user_id = users[0][0]
                    # есть ли у него записи?
                    cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                    user_appointment = cursor.fetchall()
                    if len(user_appointment) > 0:
                        have_appointment = True
                    else:
                        have_appointment = False

            return {"user_id": user_id, 
                    "phone_number": slot_value, 
                    "have_appointment": have_appointment}      
                      
        else:
            dispatcher.utter_message(text = 'неверный формат номера 2')
            return {"user_id": None, 
                    "phone_number": None, 
                    "have_appointment": None} 


class ActionPhoneNumber(Action): 
 
    def name(self) -> Text:            
        return "action_phone_number"
 
    def run(self, dispatcher:CollectingDispatcher, tracker:Tracker, domain:Dict[Text,Any]) -> List[Dict[Text, Any]]:
        # slot_value  = tracker.get_slot("phone_number")
 
        text  = tracker.latest_message['text']
        try:
            c = int(text)
        except:
            dispatcher.utter_message(text = 'неверный формат номера')
            return [SlotSet("user_id", None),
                    SlotSet("have_appointment", None)]
        
        if c > 999999999 and c < 10000000000:
            cursor.execute('SELECT * FROM user WHERE phone = ?',(c,))
            users = cursor.fetchall()
            if len(users) > 0:
                user_id = users[0][0]
                # есть ли у него записи?
                cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                user_appointment = cursor.fetchall()
                if len(user_appointment) > 0:
                    have_appointment = True
                else:
                    have_appointment = False
            else:
                # Добавляем нового пользователя
                cursor.execute('INSERT INTO user (phone) VALUES (?)', (c,))
                connection.commit()
                cursor.execute('SELECT * FROM user WHERE phone = ?',(c,))
                users = cursor.fetchall()
                if len(users) > 0:
                    user_id = users[0][0]
                    # есть ли у него записи?
                    cursor.execute('SELECT * FROM appointment WHERE user = ?',(user_id,))
                    user_appointment = cursor.fetchall()
                    if len(user_appointment) > 0:
                        have_appointment = True
                    else:
                        have_appointment = False
        else:
            dispatcher.utter_message(text = 'неверный формат номера')
            return [SlotSet("user_id", None),
                    SlotSet("have_appointment", None)]
        
        return [SlotSet("user_id", user_id),
                SlotSet("have_appointment", have_appointment)]