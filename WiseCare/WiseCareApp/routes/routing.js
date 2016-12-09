exports.form_validation = function(req, res){
  res.render('form_validation', { title: 'Reviews' });
};

exports.general = function(req, res){
	  res.render('ageBased', { title: 'Reviews' });
};

exports.stateBased = function(req, res){
	  res.render('stateBased', { title: 'State Based' });
};

exports.metal = function(req, res){
	  res.render('metal', { title: 'Metal' });
};

exports.premiumDiff = function(req, res){
	  res.render('premiumDiff', { title: 'Premium Difference' });
};
exports.deaths = function(req, res){
	  res.render('deaths', { title: 'Deaths Per Year' });
};
	
