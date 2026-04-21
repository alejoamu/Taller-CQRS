package com.techbank.account.query.domain;

import com.techbank.cqrs.core.domain.BaseEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface AccountRepository extends CrudRepository<BankAccount, String> {
    List<BankAccount> findAllByAccountHolder(String accountHolder);
    List<BaseEntity> findByBalanceGreaterThan(double balance);
    List<BaseEntity> findByBalanceLessThan(double balance);
}
