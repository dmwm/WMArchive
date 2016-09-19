// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.FilterView = Backbone.View.extend({

  // Definition of the rendered HTML element
  tagName: 'fieldset',
  className: 'filter-container form-group',

  // Content template of the rendered HTML element
  template: _.template(`
      <div class="dropdown form-group">
        <label class="form-control-label" for="<%=input_id%>">
          <%=label%>
        </label>
        <div class="filter-text-container">
          <input type="search" class="form-control filter-text" id="<%=input_id%>" placeholder="<%=placeholder%>">
          <span class="clear-filter">✕</span>
        </div>
      </div>
    `),

  // Initialize view object
  initialize: function(options) {
    // Expects a dictionary with `label` and `input_id` keys on initialization
    _.extend(this, _.pick(options, 'label', 'input_id'));

    // Listen to scope changes and re-render
    this.model = app.scope;
    this.listenTo(this.model, 'change:' + this.input_id, this.render);
    this.listenTo(this.model, 'change:suggestions', this.render);
  },

  // Render view to HTML
  render: function() {
    var scope_key = this.input_id;
    // Always retrieve Backbone.Model attributes with `get`.
    var scope_value = this.model.get(scope_key);

    // Render content template
    this.$el.html(this.template({
      label: this.label.toUpperCase(),
      input_id: this.input_id,
      placeholder: "Filter by " + this.label + "…"
    }));


    // Render suggestions dropdown menu
    // First, retrieve descriptions for suggestions if available
    var descriptions = null;
    if (scope_key == 'exitCode') {
      descriptions = (this.model.get('supplementaryData') || {})['exitCodes'];
    }
    // Retrieve suggestions data and combine it with descriptions
    var suggestions = ((this.model.get('suggestions') || {})[scope_key] || []).sort().map(function(suggestion) {
      return {
        value: suggestion,
        description: (descriptions || {})[suggestion] || null,
      };
    });
    // Setup suggestions dataset
    var all_selection_options = new Bloodhound({
      datumTokenizer: Bloodhound.tokenizers.whitespace,
      queryTokenizer: Bloodhound.tokenizers.whitespace,
      local: suggestions,
    });
    // Setup function to show all suggestions if no input is in textfield
    function selection_options(q, sync) {
      if (q === '') {
        sync(all_selection_options.all());
      } else {
        all_selection_options.search(q, sync);
      }
    }
    // Render suggestions dropdown menu
    this.$('.filter-text').typeahead({
      hint: true,
      highlight: true,
      minLength: 0,
      classNames: {
        menu: 'dropdown-menu',
        suggestion: 'dropdown-item',
        cursor: 'dropdown-cursor',
      }
    },
    {
      name: this.input_id + '-options',
      source: selection_options,
      display: function(suggestion) {
        return suggestion.value;
      },
      limit: 200,
      templates: {
        suggestion: _.template('<div><%=value%><% if (description != null) { %><small class="text-muted"> - <%=description%></small><% } %></div>')
      },
    });

    // Toggle active state to style differently if the filter is active
    this.$('.filter-text').val(scope_value);
    if (scope_value != null && scope_value != '') {
      this.$el.addClass('filter-active');
      this.$('.clear-filter').show();
    } else {
      this.$el.removeClass('filter-active');
      this.$('.clear-filter').hide();
    }

    // Required to trigger events when added to parent view
    this.delegateEvents();

    // Always return `this` in `render` to allow functional chaining
    return this;
  },

  // User interaction events to listen to
  events: {
    'typeahead:select .filter-text': 'applyFilter',
    'keyup .filter-text': 'keyup',
    'focusout .filter-text': 'focusout',
    'click .clear-filter': 'clearFilter',
  },

  applyFilter: function(event, selection) {
    var scope_key = this.input_id;
    var scope_value = selection;

    if (scope_value == '') {
      scope_value = null;
    }

    this.model.set(scope_key, scope_value);
  },

  clearFilter: function(event, selection) {
    var scope_key = this.input_id;
    this.model.set(scope_key, null);
  },

  keyup: function(event) {
    if (event.keyCode == 13) {
      this.applyFilter(event, event.target.value);
    }
  },

  focusout: function(event) {
    this.applyFilter(event, event.target.value);
  },

});
