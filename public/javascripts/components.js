class Console extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            filter: this.emptySearchFilter
        }
    }

    emptySearchFilter = { string: "search", regex: null };

    render() {
        return (
            <div id="console">
                <Search filter={this.state.filter} setFilter={this.setFilter(this)}/>
                <div id="topic-section">
                    <Topics topics={this.props.topics} filter={this.state.filter}/>
                </div>
            </div>
        )
    }

    setFilter(self) {
        return (event) => {
            event.persist();
            self.setState(state => {
                state.filter = event.target.value ? { string: event.target.value, regex: new RegExp(event.target.value, 'i') } : this.emptySearchFilter;
                return state;
            });
        };
    }
}

class Search extends React.Component {
    render() {
        return (
            <input id="search-input" className="topic-name-text" type="search" value={this.props.filter.string} onChange={this.props.setFilter} onFocus={this.handleFocus}/>
        )
    }

    handleFocus(event) {
        event.target.select()
    }
}

class Topics extends React.Component {
    render() {
        let searchFilter = (topic) => {
            return this.props.filter.regex ? topic.name.match(this.props.filter.regex) !== null : true;
        };

        let t = this.props.topics.filter(searchFilter).map(topic => {
            return (<Topic key={topic.name} topicName={topic.name}/>)
        });
        return (
            <div id="topics-scrollbar-facade">
                <div id="topics"> {t} </div>
            </div>
        )
    }
}

class Topic extends React.Component {
    render() {
        return (<div className="topic-name topic-name-text">{this.props.topicName}</div>)
    }
}

fetch('/topics').then(response => {
    response.json().then(json => {
        ReactDOM.render(React.createElement(Console, {'topics': json.topics}), document.getElementById('main'))
    });
});

