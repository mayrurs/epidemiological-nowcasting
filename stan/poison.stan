data {
  int T;              // Number of rows in reporting trapezoid 
  int D;              // Maximum delay
  int rt[T, D];    // Reporting trapezoid (Including zero delay)
   // prior parameter
  matrix[T, D] Z;        // Matrix indicating non-reporting days
  vector[D] theta;  // Parameters of Dirichlet prior for baseline delay distribution
  vector[2] normal;
  vector[2] gamma // Gamma parameters
  ; // Gamma parameters
}

parameters {
 
  vector[T] alpha; // Effect of time point t 
  simplex[D] beta; // delay probabilities
  // real<lower=0, upper=2> tau; // Variance parameter for random walk
  real tau;
}

transformed parameters  {
  
  matrix[T, D] logLambda; // expected number of cases at time t and delay d
   
  for (t in 1:T) {
    for (d in 1:D) {
      logLambda[t, d] = alpha[t] + log(beta[d]);
    }
  }
    
}
  

model {
  
  // Priors
 tau ~ gamma(gamma[1], gamma[2]); // Variance
 beta ~ dirichlet(theta); 
 
 // Random walk 
  alpha[1] ~ normal(normal[1], normal[2]);
  for(t in 2:T) {
    alpha[t] ~ normal(alpha[t-1], tau);
  }
  
  // Likelihood
  for (t in 1:T) {
    for (d in 1:min(T - t + 1, D)){
       // Only compute likelihood for reported
         rt[t, d] ~ poisson(exp(logLambda[t, d]));
    }
  }
}

generated quantities {
  int n[T, D];
  int N[T];
  for (t in 1:T) {
    for (d in 1:D) {
      if (t + d - 1 <= T) {
        // Keep reported values 
        n[t, d] = rt[t, d];
      } else {
        // Predict missing values
        n[t, d] = poisson_rng(exp(logLambda[t, d]));
    
      }
    }
    N[t] = sum(n[t, ]);
  }
}