class Console extends React.Component {

    static topicSocket = null;
    static PollConsumer = {"ConsumerMessage": "PollConsumer"};

    constructor(props) {
        super(props);
        this.state = {
            filter: this.emptySearchFilter,
            messages: []
        }
    }

    emptySearchFilter = {string: "search", regex: null};

    render() {
        return (
            <div id="console">
                <Search filter={this.state.filter} setFilter={(event) => this.setFilter(event)}/>
                <div id="topic-section">
                    <Topics topics={this.props.topics} filter={this.state.filter}
                            subscribeToTopic={(topicName) => this.subscribeToTopic(topicName)}/>
                    <MessageView messages={this.state.messages} pollTopic={() => this.pollTopic()}/>
                </div>
            </div>
        )
    }

    setFilter(event) {
        event.persist();
        this.setState(state => {
            state.filter = event.target.value ? { string: event.target.value, regex: new RegExp(event.target.value, 'i') } : this.emptySearchFilter;
            return state;
        })
    }

    pushMessage(message) {
        this.setState(state => {
            state.messages.push(message);
            return state;
        })
    }

    clearMessages() {
        this.setState(state => {
            state.messages = [];
            return state;
        })
    }

    subscribeToTopic(topicName) {
        if (Console.topicSocket !== null) Console.topicSocket.close();

        let ws = new WebSocket("ws://" + location.host + "/topic/" + topicName + "/string");

        ws.onopen = () => {
            this.clearMessages();
            ws.send(JSON.stringify(Console.PollConsumer));
        };

        ws.onmessage = (evt) => {
            let data = JSON.parse(evt.data);
            data.payload.forEach(message => this.pushMessage(message));
            Object.entries(Console.lastOffsets(data.payload)).forEach(entry =>{
              fetch("/commit/" + topicName + "/" + entry[0] + "/" + entry[1], { method: 'PUT', credentials: 'include' })
            })
        };

        Console.topicSocket = ws;
    }

    static lastOffsets(payload) {
        let offsets = {};
        payload.forEach(message => {
           if (offsets.hasOwnProperty(message.partition)) {
               if (offsets[message.partition] < message.offset) {
                   offsets[message.partition] = message.offset;
               }
           } else {
               offsets[message.partition] = message.offset;
           }
        });
        return offsets;
    }

    pollTopic() {
        if (Console.topicSocket !== null) Console.topicSocket.send(JSON.stringify(Console.PollConsumer));
    }
}

class Search extends React.Component {
    render() {
        return (
            <input id="search-input" className="topic-name-text" type="search" value={this.props.filter.string}
                   onChange={this.props.setFilter} onFocus={Search.handleFocus}/>
        )
    }

    static handleFocus(event) {
        event.target.select()
    }
}

class Topics extends React.Component {
    render() {
        let searchFilter = (topic) => {
            return this.props.filter.regex ? topic.name.match(this.props.filter.regex) !== null : true;
        };

        let topics = this.props.topics.filter(searchFilter).map(topic => {
            return (<Topic key={topic.name} topicName={topic.name} subscribeToTopic={this.props.subscribeToTopic}/>)
        });

        return (
            <div id="topics-scrollbar-facade">
                <div id="topics"> {topics} </div>
            </div>
        )
    }
}

class Topic extends React.Component {
    render() {
        return (
            <div className="topic-name topic-name-text" onClick={() => this.props.subscribeToTopic(this.props.topicName)}>{this.props.topicName}</div>)
    }
}

class MessageView extends React.Component {

    static DomId = "message-view";

    render() {
        let messages = this.props.messages.map(message => {
            return <Message message={message}/>
        });

        return (
            <div id="message-view-scrollbar-facade">
                <div id={MessageView.DomId}>
                    <ul>{messages}</ul>
                </div>
            </div>
        )
    }

    componentDidMount() {
        let messageView = document.getElementById(MessageView.DomId);
        messageView.addEventListener('scroll', (event) => {
            if (event.target.scrollTop/event.target.scrollHeight > 0.5) this.props.pollTopic()
        })
    }


    componentWillUnmount() {
        document.getElementById(MessageView.DomId).removeEventListener('scroll', null)
    }
}

class Message extends React.Component {
    render() {
        return (
            <li>
                <div className="card">
                    <div className="card-content">
                        {this.props.message.value}
                    </div>
                    <div className="card-action">
                        <div className="chip">key: {this.props.message.key}</div>
                        <div className="chip">partition {this.props.message.partition}</div>
                        <div className="chip">offset {this.props.message.offset}</div>
                    </div>
                </div>
            </li>
        )
    }
}

fetch('/topics').then(response => {
    response.json().then(json => {
        ReactDOM.render(React.createElement(Console, {'topics': json.topics}), document.getElementById('main'))
    });
});

