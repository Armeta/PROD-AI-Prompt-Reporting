# Visualizations 
import streamlit as st
from   PIL import Image
import sys


from streamlit_extras.switch_page_button import switch_page
# add src to system path
sys.path.append('src')

# custom functions
from lib import code_library
import json
# tab icon
image = Image.open('src/media/armeta-icon.png')

# Page Config
st.set_page_config(
    page_title="Retail Analytics Digital Assistant",
    page_icon=image,
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "This is a webpage with a user input box where to input natural language and recieve real information along with links to a dashboard to help satisfy your query"
    }
)


def main():

    # Get connection string paramaters
    connectionString = open('src/json/connection_details.json', "r")
    connectionString = json.loads(connectionString.read())
    session          = code_library.snowconnection(connectionString)    
    
    # gets mapping file and their encodings as well as meta data for the model being used
    model, dash_enc, dash_opts, query_enc, query_opts, BotAvatar, UserAvatar = code_library.env_Setup(session)
    
    # (re)-initialize current chat 
    if 'messages' not in st.session_state:
        st.session_state['messages'] = []

    #manage feedback session state
    if 'visibility' not in st.session_state:
        st.session_state.visibility     = 'visible'
        st.session_state.disabled       = True
        st.session_state.horizontal     = True
        st.session_state.FeedbackRating = ''
        st.session_state.FeedbackText   = ''

    # sidebar options
    with st.sidebar:
         # caching for chat
        number = code_library.manage_Cache()

         # Give filtering options for AI results
        options = st.radio("What would you like to see?",('Both Dashboard and Query Results', 'Dashboards Only', 'Query Results Only'))

    # load chat history 
    code_library.load_Cache(UserAvatar, BotAvatar)
   
    # recieve prompt from user
    prompt = st.chat_input("Send a Message")  
    test = ''
    if prompt : 

        st.session_state.disabled       = False
        # Start Chat - user
        with st.chat_message("user", avatar = UserAvatar):
            st.markdown(prompt)
        
        # clean the prompt before the AI recieves it
        clean_prompt = prompt.replace('\'','').replace('-',' ')
            
        # run the prompt against the AI to recieve an answer And Write to session cache for user
        dash_answer, query_answer = \
        code_library.do_Get(clean_prompt, model, dash_enc, dash_opts, query_enc, query_opts)        
        code_library.save_UserCache(number, prompt)               
        #code_library.write_Audit(session, prompt)

        #Start chat - assistant
        with st.chat_message("assistant", avatar = BotAvatar):
            # Show query result 
            if(query_answer != '') and (options != 'Dashboards Only'):
                # Write results + session cache for assistant
                query_answer = str(query_answer).replace("$", "\\$")
                st.markdown(query_answer) 
                code_library.save_AssistantCache(number, query_answer)
            elif (options != 'Dashboards Only'):
                # Write results + session cache for assistant 
                st.write("No query results")               
                code_library.save_AssistantCache(number, "No query results")

            # Show dashboard result  
            if(dash_answer != '') and (options != 'Query Results Only'): 
                # Write results + session cache for assistant
                st.markdown("Your query reminds me of this [dashboard.](%s)" % dash_answer)                
                code_library.save_AssistantCache(number, "Your query reminds me of this [dashboard.](%s)" % dash_answer)
        # End chat - assistant
    # ask user if reply was helpful
    if st.button('Give Feedback'):
        switch_page('Give Feedback')
 

 
image.close()

if __name__ == '__main__':  
    main()