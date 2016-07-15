var app = app || {};

app.FooterView = Backbone.View.extend({

  template: _.template('<div class="row"><div class="col-sm-6" id="statusView"></div><div class="col-sm-6" id="jobsView"></div></div><div class="row"><div class="col-sm-12" style="text-align: center">CMS Collaboration | <%=today%></div></div>'),

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
