var app = app || {};

app.NavMainView = Backbone.View.extend({

    initialize: function(options) {
        Backbone.history.on('route', function(source, path) {
            this.render(path);
        }, this);
    },

    titles: {
        "performance": "Performance",
        "apis": "APIs",
    },

    events: {
        'click a': function(source) {
            var href = source.target.getAttribute('href');
            Backbone.history.navigate(href, { trigger: true });
            return false;
        }
    },

    render: function(route) {
      this.$el.empty();
      var template = _.template('<li class="nav-item <%=active%>"><a class="nav-link" href="<%=url%>"><%=title%></a></li>');
      for (var key in this.titles) {
        this.$el.append(template({ url: key == 'home' ? '' : key, title: this.titles[key], active: route === key ? 'active' : '' }));
      }
    },

});
