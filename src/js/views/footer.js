// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.FooterView = Backbone.View.extend({

  template: _.template(`
    <div class="container-fluid">
      <div class="row">
        <div class="col-sm-12" style="text-align: center">
          <a href="http://cms.web.cern.ch" target="_blank">CMS Collaboration</a> | <a href="https://github.com/dmwm/WMArchive" target="_blank">WMArchive GitHub Repository</a> | Performance aggregation UI by <a href="https://github.com/knly" target="_blank">Nils Leif Fischer</a> | <a href="https://github.com/knly/WMArchiveAggregation" target="_blank">Documentation</a>
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
