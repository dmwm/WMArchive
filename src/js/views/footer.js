var app = app || {};

app.FooterView = Backbone.View.extend({

  template: _.template(`
    <div class="container">
      <div class="row">
        <div class="col-sm-12" style="text-align: center">
          <a href="http://cms.web.cern.ch" target="_blank">CMS Collaboration</a> | <a href="https://github.com/dmwm/WMArchive" target="_blank">WMArchive GitHub Repository</a>
        </div>
      </div>
    <div>
  `),

  initialize: function() {
  },

  render: function(){
    this.$el.html(this.template());
  }

});
