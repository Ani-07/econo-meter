
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import plotly.express as px
import bs4
from bs4 import BeautifulSoup
import boto3

#Loading and formatting Data
###############################################################################################
# Load data

def Econ_HTML(event, context):

    bucket = "twitsent1"
    file_name = "Tweet_Log.csv"
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket, Key= file_name) 
    tweets = pd.read_csv(obj['Body']) # 'Body' is a key word 
    
    #Format Data for Historical Sent
    
    tweets['Time'] = [datetime.strptime(tweets.iloc[i,0][0:len(tweets.iloc[i,0])-4],'%Y-%m-%d %H:%M:%S') for i in range(tweets.shape[0])]
    
    tweets['Pos Sent'] = [tweets.iloc[i,-2]*.01 for i in range(tweets.shape[0])]
    tweets['Neg Sent'] = [tweets.iloc[i,-1]*.01 for i in range(tweets.shape[0])]
    
    #Format Data for Current Sent
    
    tweets_analyzed = "Tweets Analyzed:{} Tweets".format(tweets.iloc[-1,5])
    print("Analyzed", tweets_analyzed)
    last_up = "Last Updated: {} GMT".format(tweets.iloc[-1,0])
    print("update", last_up) 
    tweets_current = pd.DataFrame(columns = ("Sentiment", "percentage"))
    
    pos_sent = {"Sentiment": "Positive", "percentage": tweets.iloc[-1,-2]} 
    print("pos", pos_sent)
    neg_sent = {"Sentiment": "Negative", "percentage": tweets.iloc[-1,-1]}
    print("neg", neg_sent)    
    tweets_current = tweets_current.append(pos_sent, ignore_index = True)
    tweets_current = tweets_current.append(neg_sent, ignore_index = True)
    
    #Creating the Hist Sent Plot
    ###############################################################################################
    
    # Create figure
    hist_sent = go.Figure()
    
    hist_sent.add_trace(
        go.Scatter(x=list(tweets.Time), y=list(tweets['Pos Sent']), name = "Positive Sentiment Percentage"))
    
    # Set title
    hist_sent.update_layout(
        yaxis= dict(
        tickformat= ',.0%'),
        title_text="Historical Twitter Sentiment",
        title_font_color="black",
        yaxis_title="Positive Sentiment (%)")
    
    # Add range slider
    hist_sent.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            type="date"
        )
    )
    
    #Creating the current Sent Plot
    ###############################################################################################
    
    current_sent = px.pie(tweets_current, values='percentage', names='Sentiment', color='Sentiment', 
                 color_discrete_map={'Positive':'RoyalBlue',
                                     'Negative':'IndianRed'})
    
    current_sent.update_traces(textposition='inside', textinfo='percent', textfont_size=20)             
    
    current_sent.update_layout(
        annotations=[
            go.layout.Annotation(
                showarrow=False,
                text=last_up,
                xanchor='left',
                x=0.6,
                yanchor='top',
                y=0.05),
            go.layout.Annotation(
                showarrow=False,
                text= tweets_analyzed,
                xanchor='left',
                x=0,
                yanchor='top',
                y=0.05
            )])
    
    
    hist_sent.show()
    
    current_sent.show()
    
    #Saving the plot as HTML
    ###############################################################################################
    
    hist = hist_sent.to_html(include_plotlyjs='cdn')
    
    current = current_sent.to_html(include_plotlyjs='cdn')
    
    bucket = "econometer"
    file_name = "Sent_Template_Hist_input.html"
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket, Key= file_name)
    txt = obj['Body'].read().decode('utf-8') 
    
    soup = bs4.BeautifulSoup(txt)
    soup.find("div", {"id": "graph"}).append(BeautifulSoup(hist[87:-30], 'html.parser'))
    
    file_name = 'Sent_Template_Hist.html'
    s3.put_object(Bucket = bucket, Key = file_name, Body = str(soup), ContentType = "text/html")
    
    file_name = "Sent_Template_Home_input.html"
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket, Key= file_name)
    txt = obj['Body'].read().decode('utf-8') 
    
    soup = bs4.BeautifulSoup(txt)
    soup.find("div", {"id": "pie"}).append(BeautifulSoup(current[87:-30], 'html.parser'))
    
    file_name = 'Sent_Template_Home.html'
    s3.put_object(Bucket = bucket, Key = file_name, Body = str(soup), ContentType = "text/html")
    
    
    
    
    