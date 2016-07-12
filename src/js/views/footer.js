var app = app || {};

app.FooterView = Backbone.View.extend({

  template: _.template('<div class="row centered" style="font-size: small"><div class="col col-11"><div class="row"><div class="col col-6" id="statusView"></div><div class="col col-6" id="jobsView"></div></div><div class="row"><div class="col col-12" style="text-align: center">CMS Collaboration | <%=today%></div></div></div></div>'),

  initialize: function() {
    this.statusView = new app.StatusView();
    this.jobsView = new app.JobsView();
  },

  render: function(){
    this.$el.html(this.template({ today: new Date() }));
    this.statusView.setElement(this.$('#statusView')).render();
    this.jobsView.setElement(this.$('#jobsView')).render();
  }

});
